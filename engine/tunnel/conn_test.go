package tunnel_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/tunnel"
)

var _ = Describe("NewStreamConn", func() {
	var a, b tunnel.Conn
	var rawA, rawB net.Conn

	BeforeEach(func() {
		rawA, rawB = net.Pipe()
		a, b = tunnel.NewStreamConn(rawA), tunnel.NewStreamConn(rawB)
		DeferCleanup(func() {
			_ = a.Close()
			_ = b.Close()
		})
	})

	It("delivers frames whole and in order, including empty and large ones", func() {
		large := bytes.Repeat([]byte{7}, 1<<20)
		frames := [][]byte{[]byte("one"), {}, large, []byte("last")}
		go func() {
			defer GinkgoRecover()
			for _, f := range frames {
				Expect(a.Send(f)).To(Succeed())
			}
		}()
		for _, want := range frames {
			got, err := b.Receive()
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want))
		}
	})

	It("reports io.EOF between frames and io.ErrUnexpectedEOF inside one", func() {
		go func() {
			defer GinkgoRecover()
			Expect(a.Send([]byte("whole"))).To(Succeed())
			_ = a.Close()
		}()
		_, err := b.Receive()
		Expect(err).NotTo(HaveOccurred())
		_, err = b.Receive()
		Expect(err).To(MatchError(io.EOF))

		rawC, rawD := net.Pipe()
		d := tunnel.NewStreamConn(rawD)
		defer d.Close()
		go func() {
			var prefix [4]byte
			binary.BigEndian.PutUint32(prefix[:], 10)
			_, _ = rawC.Write(prefix[:])
			_, _ = rawC.Write([]byte("short"))
			_ = rawC.Close()
		}()
		_, err = d.Receive()
		Expect(err).To(MatchError(io.ErrUnexpectedEOF))
	})

	It("refuses frames above MaxFrameSize in both directions", func() {
		Expect(a.Send(make([]byte, tunnel.MaxFrameSize+1))).To(MatchError(tunnel.ErrFrameTooLarge))

		go func() {
			var prefix [4]byte
			binary.BigEndian.PutUint32(prefix[:], tunnel.MaxFrameSize+1)
			_, _ = rawA.Write(prefix[:])
		}()
		_, err := b.Receive()
		Expect(err).To(MatchError(tunnel.ErrFrameTooLarge))
	})

	It("never interleaves frames from concurrent senders", func() {
		const senders, each = 8, 50
		var wg sync.WaitGroup
		for s := range senders {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				for i := range each {
					Expect(a.Send([]byte(fmt.Sprintf("%d:%d:%s", s, i, bytes.Repeat([]byte("x"), 1000))))).To(Succeed())
				}
			}()
		}
		seen := map[string]bool{}
		for range senders * each {
			got, err := b.Receive()
			Expect(err).NotTo(HaveOccurred())
			var s, i int
			var tail string
			_, err = fmt.Sscanf(string(got), "%d:%d:%s", &s, &i, &tail)
			Expect(err).NotTo(HaveOccurred())
			Expect(tail).To(Equal(string(bytes.Repeat([]byte("x"), 1000))))
			seen[fmt.Sprintf("%d:%d", s, i)] = true
		}
		wg.Wait()
		Expect(seen).To(HaveLen(senders * each))
	})
})
