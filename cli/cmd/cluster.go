package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
)

// ClusterData is exported for testing
type ClusterData struct {
	Cluster    *cluster.Cluster
	Output     *cluster.RemoteOutput
	NumHosts   int
	connection *dbconn.DBConn
}

func createCobraCommand(use string, short string, cmd *command) *cobra.Command {
	if cmd == nil {
		return &cobra.Command{Use: use, Short: short}
	}
	return &cobra.Command{
		Use:   use,
		Short: short,
		Run: func(cobraCmd *cobra.Command, args []string) {
			clusterData, err := doSetup()
			if err == nil {
				err = clusterRun(cmd, clusterData)
			}
			exitWithReturnCode(err)
		},
	}
}

var (
	clusterCmd  = createCobraCommand("cluster", "Perform <command> on each segment host in the cluster", nil)
	initCmd     = createCobraCommand("init", "(deprecated) Install PXF extension under $GPHOME on master, standby master, and all segment hosts", &InitCommand)
	startCmd    = createCobraCommand("start", "Start the PXF server instances on master, standby master, and all segment hosts", &StartCommand)
	stopCmd     = createCobraCommand("stop", "Stop the PXF server instances on master, standby master, and all segment hosts", &StopCommand)
	statusCmd   = createCobraCommand("status", "Get status of PXF servers on master, standby master, and all segment hosts", &StatusCommand)
	syncCmd     = createCobraCommand("sync", "Sync PXF configs from master to standby master and all segment hosts. Use --delete to delete extraneous remote files", &SyncCommand)
	resetCmd    = createCobraCommand("reset", "(deprecated) No operation", &ResetCommand)
	registerCmd = createCobraCommand("register", "Install PXF extension under $GPHOME on master, standby master, and all segment hosts", &RegisterCommand)
	restartCmd  = createCobraCommand("restart", "Restart the PXF server on master, standby master, and all segment hosts", &RestartCommand)
	prepareCmd  = createCobraCommand("prepare", "Prepares a new base directory specified by the $PXF_BASE environment variable", &PrepareCommand)
	migrateCmd  = createCobraCommand("migrate", "Migrates configurations from older installations of PXF", &MigrateCommand)
	// DeleteOnSync is a boolean for determining whether to use rsync with --delete, exported for tests
	DeleteOnSync bool

	stdout io.Writer
	stderr io.Writer
)

func init() {
	rootCmd.AddCommand(clusterCmd)
	clusterCmd.AddCommand(initCmd)
	clusterCmd.AddCommand(startCmd)
	clusterCmd.AddCommand(stopCmd)
	clusterCmd.AddCommand(statusCmd)
	syncCmd.Flags().BoolVarP(&DeleteOnSync, "delete", "d", false, "delete extraneous files on remote host")
	clusterCmd.AddCommand(syncCmd)
	clusterCmd.AddCommand(resetCmd)
	clusterCmd.AddCommand(registerCmd)
	clusterCmd.AddCommand(restartCmd)
	clusterCmd.AddCommand(prepareCmd)
	clusterCmd.AddCommand(migrateCmd)

	stdout = os.Stdout
	stderr = os.Stderr
}

func SetStdout(w io.Writer) {
	stdout = w
}

func SetStderr(w io.Writer) {
	stderr = w
}

func exitWithReturnCode(err error) {
	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func handlePlurality(num int) string {
	if num == 1 {
		return ""
	}
	return "s"
}

// GenerateStatusReport exported for testing
func GenerateStatusReport(cmd *command, clusterData *ClusterData) {
	if _, ok := cmd.messages[standby]; !ok {
		// this command cares not about standby
		msg := fmt.Sprintf(cmd.messages[status], clusterData.NumHosts, handlePlurality(clusterData.NumHosts))
		fmt.Fprint(stdout, msg)
		gplog.Info(msg)
		return
	}
	standbyMsg := ""
	numHosts := clusterData.NumHosts
	if cmd.whereToRun&cluster.INCLUDE_MASTER == cluster.INCLUDE_MASTER {
		numHosts--
	}
	if isStandbyAloneOnHost(clusterData) {
		standbyMsg = cmd.messages[standby]
		numHosts--
	}
	msg := fmt.Sprintf(cmd.messages[status], standbyMsg, numHosts, handlePlurality(numHosts))
	fmt.Fprint(stdout, msg)
	gplog.Info(msg)
}

// GenerateOutput is exported for testing
func GenerateOutput(cmd *command, clusterData *ClusterData) error {
	numErrors := clusterData.Output.NumErrors
	if numErrors == 0 {
		msg := fmt.Sprintf(cmd.messages[success], clusterData.NumHosts-numErrors, clusterData.NumHosts, handlePlurality(clusterData.NumHosts))
		fmt.Fprintln(stdout, msg)
		gplog.Info(msg)
		return nil
	}
	response := ""
	for _, failedCommand := range clusterData.Output.FailedCommands {
		if failedCommand == nil {
			continue
		}
		host := failedCommand.Host
		errorMessage := failedCommand.Stderr
		if len(errorMessage) == 0 {
			errorMessage = failedCommand.Stdout
		}
		lines := strings.Split(errorMessage, "\n")
		errorMessage = lines[0]
		if len(lines) > 1 {
			errorMessage += "\n" + lines[1]
		}
		if len(lines) > 2 {
			errorMessage += "..."
		}
		response += fmt.Sprintf("%s ==> %s\n", host, errorMessage)
	}
	msg := fmt.Sprintf(cmd.messages[err], numErrors, clusterData.NumHosts, handlePlurality(clusterData.NumHosts))
	gplog.Error(msg)
	gplog.Error(response)
	fmt.Fprintf(stderr, "ERROR: %s", msg)
	fmt.Fprint(stderr, response)
	return errors.New(response)
}

func doSetup() (*ClusterData, error) {
	connection := dbconn.NewDBConnFromEnvironment("postgres")
	err := connection.Connect(1)
	if err != nil {
		msg := fmt.Sprintf("Could not connect to GPDB.\n%s\n"+
			"Please make sure that your Greenplum database is running and you are on the master node.", err.Error())
		fmt.Fprintln(stderr, msg)
		gplog.Error(msg)
		return nil, err
	}
	segConfigs, err := cluster.GetSegmentConfiguration(connection, true)
	if err != nil {
		msg := fmt.Sprintf("Could not retrieve segment information from GPDB.\n%s\n" + err.Error())
		fmt.Fprintln(stderr, msg)
		gplog.Error(msg)
		return nil, err
	}
	clusterData := &ClusterData{Cluster: cluster.NewCluster(segConfigs), connection: connection}

	return clusterData, nil
}

func clusterRun(cmd *command, clusterData *ClusterData) error {
	defer clusterData.connection.Close()

	err := cmd.Warn(os.Stdin)
	if err != nil {
		fmt.Fprintln(stderr, err.Error())
		gplog.Info(err.Error())
		return err
	}

	functionToExecute, err := cmd.GetFunctionToExecute()
	if err != nil {
		fmt.Fprintln(stderr, err.Error())
		gplog.Error(err.Error())
		return err
	}

	commandList := clusterData.Cluster.GenerateSSHCommandList(cmd.whereToRun, functionToExecute)
	clusterData.NumHosts = len(commandList)
	gplog.Debug(fmt.Sprintf("Running %q on %d hosts", functionToExecute("HOSTNAME"), clusterData.NumHosts))
	GenerateStatusReport(cmd, clusterData)
	clusterData.Output = clusterData.Cluster.ExecuteClusterCommand(cmd.whereToRun, commandList)
	gplog.Debug("Remote command output - scope=%d", clusterData.Output.Scope)
	for _, command := range clusterData.Output.Commands {
		isFailed := command.Error != nil
		gplog.Debug("host=%s, command=%q, failed=%t, stdout=%q", command.Host, command.CommandString, isFailed, command.Stdout)
		if isFailed {
			gplog.Debug("stderr=%q", command.Stderr)
		}
	}
	return GenerateOutput(cmd, clusterData)
}

func isStandbyAloneOnHost(clusterData *ClusterData) bool {
	standbyHost := clusterData.Cluster.GetHostForContent(-1, "m")
	if standbyHost == "" {
		return false // there is no standby master
	}
	return len(clusterData.Cluster.GetContentsForHost(standbyHost)) == 1
}
