package server

import (
	"encoding/json"
	"os"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
	Policies        struct {
		Create string `json:"create"`
		Update string `json:"update"`
		Delete string `json:"delete"`
	} `json:"policies"`
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
			info := &entityInfo{
				name:            "vector",
				createPolicy:    createPolicyFromVector(tc.Policies.Create),
				updatePolicy:    updatePolicyFromVector(tc.Policies.Update),
				deletePolicy:    deletePolicyFromVector(tc.Policies.Delete),
				protectedFields: tc.Protected,
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

func createPolicyFromVector(p string) syncplanev1.CreatePolicy {
	if p == "trustedOnly" {
		return syncplanev1.CreatePolicy_CREATE_POLICY_TRUSTED_ONLY
	}
	return syncplanev1.CreatePolicy_CREATE_POLICY_ALLOW
}

func updatePolicyFromVector(p string) syncplanev1.UpdatePolicy {
	if p == "trustedOnly" {
		return syncplanev1.UpdatePolicy_UPDATE_POLICY_TRUSTED_ONLY
	}
	return syncplanev1.UpdatePolicy_UPDATE_POLICY_ALLOW
}

func deletePolicyFromVector(p string) syncplanev1.DeletePolicy {
	if p == "trustedOnly" {
		return syncplanev1.DeletePolicy_DELETE_POLICY_TRUSTED_ONLY
	}
	return syncplanev1.DeletePolicy_DELETE_POLICY_ALLOW
}
