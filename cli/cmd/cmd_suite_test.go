package cmd_test

import (
	"os/user"
	"pxf-cli/cmd"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/onsi/gomega/gbytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	testStdout  *gbytes.Buffer
	testLogFile *gbytes.Buffer
	testStderr  *gbytes.Buffer
)

func TestCmd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cmd Suite")
}

var _ = BeforeEach(func() {
	_, _, _, _, testLogFile = testhelper.SetupTestEnvironment()
	operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
	operating.System.Hostname = func() (string, error) { return "testHost", nil }
	testStdout = gbytes.NewBuffer()
	cmd.SetStdout(testStdout)
	testStderr = gbytes.NewBuffer()
	cmd.SetStderr(testStderr)
})
