package server

import (
	"encoding/json"
	"os"
	"slices"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// The conformance vectors are shared with the TS client
// (client/test/unit/writepolicy-vectors.test.ts) — both classifiers must reach
// the same verdict on every case, or one of them blocks a legal write while the
// other enqueues a write that can only be rejected.
const writePolicyVectorsPath = "../../testdata/writepolicy/vectors.json"

type writePolicyCase struct {
	Name            string  `json:"name"`
	Op              string  `json:"op"`
	ExpectedVersion *string `json:"expectedVersion"`
	// Access is nil for an entity without access rules.
	Access *struct {
		Create bool `json:"create"`
		Update bool `json:"update"`
		Delete bool `json:"delete"`
		// UpdateFields are the fields lists of the update rules when every
		// update rule has one, and nil when some update rule has none.
		UpdateFields [][]string `json:"updateFields"`
	} `json:"access"`
	KeyFields []string `json:"keyFields"`
	Protected []string `json:"protected"`
	Mask      []string `json:"mask"`
	Want      string   `json:"want"`
}

var _ = Describe("write policy conformance vectors", func() {
	var cases []writePolicyCase

	BeforeEach(func() {
		raw, err := os.ReadFile(writePolicyVectorsPath)
		Expect(err).NotTo(HaveOccurred())
		var file struct {
			Cases []writePolicyCase `json:"cases"`
		}
		Expect(json.Unmarshal(raw, &file)).To(Succeed())
		cases = file.Cases
		Expect(cases).NotTo(BeEmpty())
	})

	It("classifies every case the way the vectors say", func() {
		for _, tc := range cases {
			srv := &Server{}
			info := &entityInfo{name: "vector", protectedFields: tc.Protected}
			for _, name := range tc.KeyFields {
				info.keyFields = append(info.keyFields, vectorKeyField(name))
			}
			if a := tc.Access; a != nil {
				info.access = &entityAccess{named: opRead, rules: []accessRule{{ops: opRead}}}
				for _, op := range []struct {
					set bool
					op  accessOp
				}{{a.Create, opCreate}, {a.Update, opUpdate}, {a.Delete, opDelete}} {
					if !op.set {
						continue
					}
					info.access.named |= op.op
					if op.op != opUpdate || a.UpdateFields == nil {
						info.access.rules = append(info.access.rules, accessRule{ops: op.op})
						continue
					}
					for _, fields := range a.UpdateFields {
						info.access.rules = append(info.access.rules, accessRule{ops: opUpdate, fields: slices.Sorted(slices.Values(fields))})
					}
				}
			}
			srv.cat.Store(&catalog{entities: map[string]*entityInfo{info.name: info}})

			crud := &syncplanev1.CrudMutation{Entity: info.name, Key: "k", Op: opFromVector(tc.Op)}
			if len(tc.Mask) > 0 {
				crud.FieldMask = &fieldmaskpb.FieldMask{Paths: tc.Mask}
			}
			if tc.ExpectedVersion != nil {
				v, err := strconv.ParseUint(*tc.ExpectedVersion, 10, 64)
				Expect(err).NotTo(HaveOccurred(), tc.Name)
				crud.ExpectedVersion = &v
			}

			err := srv.checkWritePolicy(Write{Crud: crud})
			if tc.Want == "allow" {
				Expect(err).NotTo(HaveOccurred(), tc.Name)
				continue
			}
			rej, ok := AsRejection(err)
			Expect(ok).To(BeTrue(), "%s: got %v", tc.Name, err)
			Expect(rej.Code).To(Equal(CodePermissionDenied), tc.Name)
		}
	})
})

// vectorKeyField returns a string field descriptor named name, standing in for
// a key field of the vector entity: the check reads only its name.
func vectorKeyField(name string) protoreflect.FieldDescriptor {
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("vector_" + name + ".proto"),
		Package: proto.String("vector"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Vector"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   proto.String(name),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}},
		}},
	}, nil)
	if err != nil {
		panic(err)
	}
	return fd.Messages().Get(0).Fields().Get(0)
}

func opFromVector(op string) syncplanev1.CrudOp {
	switch op {
	case "PUT":
		return syncplanev1.CrudOp_CRUD_OP_PUT
	case "PATCH":
		return syncplanev1.CrudOp_CRUD_OP_PATCH
	case "DELETE":
		return syncplanev1.CrudOp_CRUD_OP_DELETE
	default:
		return syncplanev1.CrudOp_CRUD_OP_UNSPECIFIED
	}
}
