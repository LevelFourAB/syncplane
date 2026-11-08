package naming_test

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/levelfourab/syncplane/engine/internal/naming"
)

// setElemVectorsPath is the shared cross-language conformance file; the TS
// harness (client/test/unit/setelem-vectors.test.ts) runs the identical cases.
const setElemVectorsPath = "../../../testdata/setelem/vectors.json"

type setElemVectors struct {
	Cases []struct {
		Name string `json:"name"`
		Kind string `json:"kind"`
		Bits string `json:"bits"`
		Hex  string `json:"hex"`
		Want string `json:"want"`
	} `json:"cases"`
}

// valueField is the `value` field of a wrapper message: ScalarKeyString reads
// only the kind of the field it is given.
func valueField(m interface {
	ProtoReflect() protoreflect.Message
}) protoreflect.FieldDescriptor {
	return m.ProtoReflect().Descriptor().Fields().ByName("value")
}

var _ = Describe("ScalarKeyString set element vectors", func() {
	raw, err := os.ReadFile(setElemVectorsPath)
	if err != nil {
		panic(err)
	}
	var vectors setElemVectors
	if err := json.Unmarshal(raw, &vectors); err != nil {
		panic(err)
	}

	It("has cases", func() {
		Expect(vectors.Cases).NotTo(BeEmpty())
	})

	for _, c := range vectors.Cases {
		It(c.Kind+": "+c.Name, func() {
			var fd protoreflect.FieldDescriptor
			var v protoreflect.Value
			switch c.Kind {
			case "double":
				b, err := hex.DecodeString(c.Bits)
				Expect(err).NotTo(HaveOccurred())
				fd = valueField(&wrapperspb.DoubleValue{})
				v = protoreflect.ValueOfFloat64(math.Float64frombits(binary.BigEndian.Uint64(b)))
			case "float":
				b, err := hex.DecodeString(c.Bits)
				Expect(err).NotTo(HaveOccurred())
				fd = valueField(&wrapperspb.FloatValue{})
				v = protoreflect.ValueOfFloat32(math.Float32frombits(binary.BigEndian.Uint32(b)))
			case "bytes":
				b, err := hex.DecodeString(c.Hex)
				Expect(err).NotTo(HaveOccurred())
				fd = valueField(&wrapperspb.BytesValue{})
				v = protoreflect.ValueOfBytes(b)
			default:
				Fail("unknown kind " + c.Kind)
			}
			Expect(naming.ScalarKeyString(fd, v)).To(Equal(c.Want))
		})
	}
})
