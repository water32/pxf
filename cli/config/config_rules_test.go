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

		Entry("invalid empty string", "", false),

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
		Entry("invalid FQDN with trailing space", "test.com ", false),
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

var _ = Describe("PXF config validation rules", func() {
	var pxfDeployment config.PxfDeployment
	BeforeEach(func() {
		pxfDeployment = config.PxfDeployment{
			Name: "test-deployment",
			Clusters: map[string]*config.PxfCluster{
				"test-cluster-1": {
					Name:       "test-cluster-1",
					Collocated: false,
					Hosts: []*config.PxfHost{
						{Hostname: "host1.test.com"},
						{Hostname: "host2.test.com"},
					},
					Endpoint: "endpoint1.test.com",
					Groups: map[string]*config.PxfServiceGroup{
						"test-group-1": {
							Name:  "test-group-1",
							Ports: []int{1111, 2222},
						},
						"test-group-2": {
							Name:  "test-group-2",
							Ports: []int{3333, 4444},
						},
					},
				},
				"test-cluster-2": {
					Name:       "test-cluster-2",
					Collocated: false,
					Hosts: []*config.PxfHost{
						{Hostname: "host3.test.com"},
						{Hostname: "host4.test.com"},
					},
					Endpoint: "endpoint2.test.com",
					Groups: map[string]*config.PxfServiceGroup{
						"test-group-1": {
							Name:  "test-group-3",
							Ports: []int{5555, 6666},
						},
						"test-group-2": {
							Name:  "test-group-4",
							Ports: []int{7777, 8888},
						},
					},
				},
			},
		}
	})
	Context("deployment name must not be empty", func() {
		It("returns an error if name is empty", func() {
			pxfDeployment.Name = ""

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("the name of the deployment cannot be empty string")))
		})
	})
	Context("deployment must have at least one cluster", func() {
		It("returns an error if there is no cluster", func() {
			pxfDeployment.Clusters = make(map[string]*config.PxfCluster)

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("there should be at least one PXF cluster in the " +
				"PXF deployment `test-deployment`")))
		})
	})
	Context("hostnames in deployment must be unique", func() {
		It("returns an error if there are duplicate hostnames in the same cluster", func() {
			pxfDeployment.Clusters["test-cluster-1"].Hosts[0].Hostname = "test-host"
			pxfDeployment.Clusters["test-cluster-1"].Hosts[1].Hostname = "test-host"

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("host `test-host` must belong to only one pxf cluster " +
				"across the pxf deployment")))
		})
		It("returns an error if there are duplicate hostnames in different clusters", func() {
			pxfDeployment.Clusters["test-cluster-1"].Hosts[0].Hostname = "test-host"
			pxfDeployment.Clusters["test-cluster-2"].Hosts[0].Hostname = "test-host"

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("host `test-host` must belong to only one pxf cluster " +
				"across the pxf deployment")))
		})
	})
	Context("service group names in deployment must be unique", func() {
		It("returns an error if there are duplicate service group names in the same cluster", func() {
			pxfDeployment.Clusters["test-cluster-1"].Groups["test-group-1"].Name = "test-group-name"
			pxfDeployment.Clusters["test-cluster-1"].Groups["test-group-2"].Name = "test-group-name"

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("PXF group name `test-group-name` must be unique across " +
				"the pxf deployment")))
		})
		It("returns an error if there are duplicate group names in different clusters", func() {
			pxfDeployment.Clusters["test-cluster-1"].Groups["test-group-1"].Name = "test-group-name"
			pxfDeployment.Clusters["test-cluster-2"].Groups["test-group-1"].Name = "test-group-name"

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("PXF group name `test-group-name` must be unique across " +
				"the pxf deployment")))
		})
	})
	Context("there must be no more than one collocated cluster", func() {
		It("returns an error if there are duplicate group names in the same cluster", func() {
			pxfDeployment.Clusters["test-cluster-1"].Collocated = true
			pxfDeployment.Clusters["test-cluster-2"].Collocated = true

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("there must be no more than one collocated PXF cluster")))
		})
	})
	Context("cluster name must not be empty", func() {
		It("returns an error if one cluster has empty name", func() {
			pxfDeployment.Clusters["test-cluster-1"].Name = ""

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("the name of the PXF cluster cannot be empty string")))
		})
	})
	// TODO: pxfClusterIgnoreHostsWhenCollocated - How to test the UI output
	// TODO: pxfClusterIgnoreEndpointWhenCollocated - How to test the UI output
	Context("external cluster must have either hosts or endpoint specified", func() {
		It("returns an error if both host list and endpoint are empty while collocated is false", func() {
			pxfDeployment.Clusters["test-cluster-1"].Collocated = false
			pxfDeployment.Clusters["test-cluster-1"].Endpoint = ""
			pxfDeployment.Clusters["test-cluster-1"].Hosts = []*config.PxfHost{}

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("the host list and endpoint cannot be both empty " +
				"for the external PXF cluster `test-cluster-1`")))
		})
	})
	Context("endpoint must be a valid IPv4 or FQDN address", func() {
		It("returns an error if address is invalid", func() {
			pxfDeployment.Clusters["test-cluster-1"].Endpoint = "invalid-fqdn"

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("the endpoint `invalid-fqdn` of the PXF cluster `test-cluster-1` " +
				"is not a valid IPv4 address or FQDN")))
		})
	})
	Context("cluster must have at least one service group", func() {
		It("returns an error if there is no service group", func() {
			pxfDeployment.Clusters["test-cluster-1"].Groups = make(map[string]*config.PxfServiceGroup)

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("there should be at least one service group in PXF cluster `test-cluster-1`")))
		})
	})
	Context("service group name must not be empty", func() {
		It("returns an error if the service group name is empty", func() {
			pxfDeployment.Clusters["test-cluster-1"].Groups["test-group-1"].Name = ""

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("the name of the service group cannot be empty string")))
		})
	})
	Context("service group must have at least one port", func() {
		It("returns an error if there is no port", func() {
			pxfDeployment.Clusters["test-cluster-1"].Groups["test-group-1"].Ports = []int{}

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("there should be at least one port in PXF service group `test-group-1`\"")))
		})
	})
	Context("service group must have valid ports", func() {
		It("returns an error if the any port is not valid", func() {
			pxfDeployment.Clusters["test-cluster-1"].Groups["test-group-1"].Ports = []int{0, 2222, 3333}

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("invalid port number `0` under service group `test-group-1`, a port number must be between `1` - `65535`")))
		})
	})
	// TODO: pxfServiceGroupPortsWarning - How to handle the warning message
	Context("hosts must all have valid IPv4 address or FQDN as hostnames", func() {
		It("returns an error if any host address is invalid", func() {
			pxfDeployment.Clusters["test-cluster-1"].Hosts = []*config.PxfHost{
				{Hostname: "invalid-hostname"},
				{Hostname: "valid-hostname.test.com"},
			}

			err := pxfDeployment.Validate()

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errors.New("the hostname `invalid-hostname` is not a valid IPv4 address or FQDN")))
		})
	})
	Context("all validation rules are satisfied", func() {
		It("returns no error", func() {
			err := pxfDeployment.Validate()

			Expect(err).ToNot(HaveOccurred())
		})
	})
})
