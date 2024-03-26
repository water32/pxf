package config_test

import (
	"errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"pxf-cli/config"
)

var _ = Describe("validate host address", func() {
	DescribeTable("should validate IPv4 and FQDN addresses correctly",
		func(address string, expected bool) {
			Expect(config.IsValidAddress(address)).To(Equal(expected))
		},
		Entry("valid IPv4 address", "192.168.1.1", true),

		Entry("invalid IPv4 address with missing octet", "192.168.1.", false),
		Entry("invalid IPv4 address with one octet", "192", false),
		Entry("invalid IPv4 address with octet out of range", "256.2.2.2", false),
		Entry("invalid IPv4 address with too many octets", "256.2.2.2.1", false),

		Entry("IPv6 address is not allowed even it is valid", "2001:0db8:85a3:0000:0000:8a2e:0370:7334", false),
		Entry("IPv6 address with consecutive zeros is not allowed even it is valid", "2001::8a2e:0370:7334", false),

		Entry("invalid IPv6 address with missing hextet", "2001:0db8:85a3:0000:0000:8a2e:0370:", false),
		Entry("invalid IPv6 address with too many hextets", "2001:0db8:85a3:0000:0000:8a2e:0370:7334:1111", false),

		Entry("valid simple FQDN with 2 groups", "test123.com", true),
		Entry("valid simple FQDN with 3 groups", "www.test123.com", true),
		Entry("valid simple FQDN with 4 groups", "sub.domain.test123.com", true),
		Entry("valid FQDN with a hyphen", "test-info.com", true),
		Entry("valid FQDN with a hyphen and 3 groups", "sub.test-info.com", true),
		Entry("valid FQDN with a hyphen and 4 groups", "ab-info.domain.test-info.com", true),
		Entry("valid FQDN with second level domain", "test.com.cn", true),
		Entry("valid FQDN with shortest length", "t.co", true),
		Entry("valid FQDN with shortest length for 4 groups", "t.t.t.co", true),
		Entry("valid FQDN with upper case", "wwW.Test.com", true),

		Entry("invalid FQDN with only hostname", "test", false),
		Entry("invalid FQDN with 2 dots", "test.test..com", false),
		Entry("invalid FQDN with TLD length shorter than 1", "test.test.test.c", false),
		Entry("invalid FQDN separated by comma", "test,com", false),
		Entry("invalid FQDN without TLD", "test.", false),
		Entry("invalid FQDN with invalid TLD", "www.test.123", false),
		Entry("invalid FQDN with extra dot", "www.test.com.", false),
		Entry("invalid FQDN separated by space", "test com", false),
		Entry("invalid FQDN with @", "abc@test.com", false),
		Entry("invalid FQDN with space in hostname", "abc test.com", false),
		Entry("invalid FQDN with underscore in hostname", "abc_test.com", false),
	)
})

var _ = Describe("validate configuration for PXF deployment", func() {
	Context("deployment name is empty", func() {
		var pxfDeployment config.PxfDeployment
		BeforeEach(func() {
			pxfDeployment = config.PxfDeployment{
				Name:     "",
				Clusters: map[string]config.PxfCluster{},
			}
		})
		It("returns an error", func() {
			err := pxfDeployment.Validate()
			Expect(err).To(HaveOccurred())
			//fmt.Printf("%#v", err)
			Expect(err).To(Equal(errors.New("the name of the deployment cannot be empty string")))
		})
	})
})

var _ = Describe("validate configuration for PXF cluster", func() {
	Context("cluster name is empty", func() {
		BeforeEach(func() {

		})
		It("returns an", func() {

		})
	})
})

// host
// group
