package config_test

import (
	. "github.com/onsi/ginkgo/v2"
	"pxf-cli/config"
)

var _ = Describe("PXF config parse cluster.txt", func() {
	Context("file is properly set-up", func() {
		It("parses the file properly and returns the struct", func() {
			config.ParseClusterFile("../server/pxf-service/src/templates/cluster/cluster.txt")
		})
	})
})
