package cmd

import (
	"fmt"
	"io"
	"os"
	"os/user"
	"path"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/spf13/cobra"
)

var version string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "pxf",
	Version: version,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	initializeLogging()
	rootCmd.SetHelpCommand(&cobra.Command{
		Use:    "no-help",
		Hidden: true,
	})
	rootCmd.SetVersionTemplate(`{{printf "PXF version %s" .Version}}
`)
	rootCmd.SetUsageTemplate(`Usage: pxf cluster <command>
       pxf cluster {-h | --help}{{if .HasAvailableSubCommands}}

List of Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}

`)
	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("version", "v", false, "show the version of PXF server")
}

func initializeLogging() {
	executablePath, err := os.Executable()
	if err != nil {
		executablePath = "pxf_cli"
	}
	program := path.Base(executablePath)

	pxfLogDir, ok := os.LookupEnv("PXF_LOGDIR")
	if !ok || pxfLogDir == "" {
		currentUser, _ := user.Current()
		pxfLogDir = path.Join(currentUser.HomeDir, "gpAdminLogs")
	} else {
		pxfLogDir = path.Join(pxfLogDir, "admin")
	}

	createLogDirectory(pxfLogDir)
	logfile := gplog.GenerateLogFileName(program, pxfLogDir)
	logfileHandle := openLogFile(logfile)
	logger := gplog.NewLogger(io.Discard, io.Discard, logfileHandle, logfile, gplog.LOGINFO, program)
	gplog.SetLogger(logger)

	pxfLogLevel, ok := os.LookupEnv("PXF_LOG_LEVEL")
	if !ok || pxfLogLevel == "" {
		pxfLogLevel = "info"
	}
	gplog.SetLogFileVerbosity(mapToGpLogLevel(pxfLogLevel))
}

// mapToGpLogLevel converts a Log4j log level to a corresponding level from the gplog package
func mapToGpLogLevel(logLevel string) int {
	gplogLevel := gplog.LOGINFO
	// FIXME: need to handle: off, fatal, warn, trace, all
	if strings.EqualFold("error", logLevel) {
		gplogLevel = gplog.LOGERROR
	}
	if strings.EqualFold("info", logLevel) {
		gplogLevel = gplog.LOGINFO
	}
	if strings.EqualFold("debug", logLevel) {
		gplogLevel = gplog.LOGDEBUG
	}

	return gplogLevel
}

func createLogDirectory(dirname string) {
	info, err := os.Stat(dirname)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirname, 0755)
			if err != nil {
				panic(fmt.Sprintf("Cannot create log directory %s: %v", dirname, err))
			}
		} else {
			panic(fmt.Sprintf("Cannot stat log directory %s: %v", dirname, err))
		}
	} else if !info.IsDir() {
		panic(fmt.Sprintf("%s is a file, not a directory", dirname))
	}
}

func openLogFile(filename string) io.WriteCloser {
	flags := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	fileHandle, err := os.OpenFile(filename, flags, 0644)
	if err != nil {
		panic(err.Error())
	}

	return fileHandle
}
