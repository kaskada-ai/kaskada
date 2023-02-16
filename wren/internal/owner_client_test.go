package internal

import (
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("OwnerClient", func() {
	testClientID := "owner_test_client_id"

	It("should be a pleasant experience", func() {
		ownerClient := NewOwnerClient(entClient)
		Expect(ownerClient).ShouldNot(BeNil())

		createdOwner, err := ownerClient.GetOwnerFromClientID(ctx, testClientID)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(createdOwner).ShouldNot(BeNil())
		Expect(createdOwner.ClientID).Should(Equal(testClientID))

		gotOwner, err := ownerClient.GetOwner(ctx, createdOwner.ID)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(gotOwner).ShouldNot(BeNil())

		Expect(gotOwner.ID).Should(Equal(createdOwner.ID))
		Expect(gotOwner.ClientID).Should(Equal(createdOwner.ClientID))
		Expect(gotOwner.Contact).Should(Equal(createdOwner.Contact))
		Expect(gotOwner.CreatedAt).Should(BeTemporally("~", createdOwner.CreatedAt, time.Second))
		Expect(gotOwner.Name).Should(Equal(createdOwner.Name))

		missingOwner, err := ownerClient.GetOwner(ctx, uuid.New())
		Expect(err).Should(MatchError("owner not found"))
		Expect(missingOwner).Should(BeNil())
	})
})
