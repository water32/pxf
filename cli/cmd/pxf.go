package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

type envVar string

const (
	gpHome   envVar = "GPHOME"
	pxfHome  envVar = "PXF_HOME"
	pxfBase  envVar = "PXF_BASE"
	javaHome envVar = "JAVA_HOME"
	// For pxf migrate
	pxfConf envVar = "PXF_CONF"
)

type messages struct {
	success string
	status  string
	err     string
	standby string
}

type command struct {
	name commandName
	// if messages contains an entry for 'standby', then 'status'  is a printf-style format string that takes three arguments
	// if messages does not contain an entry for 'standby', then 'status' is a printf-style format string that takes
	messages   messages
	whereToRun cluster.Scope
	envVars    []envVar
}

func (cmd *command) GetFunctionToExecute() (func(string) string, error) {
	inputs, err := cmd.makeValidCliInputs()
	if err != nil {
		return nil, err
	}

	switch cmd.name {
	case sync:
		rsyncCommand := "rsync -az%[1]s -e 'ssh -o StrictHostKeyChecking=no' '%[2]s/conf' '%[2]s/lib' '%[2]s/servers' '%[3]s:%[2]s'"
		deleteString := ""
		if DeleteOnSync {
			deleteString = " --delete"
		}
		return func(hostname string) string {
			return fmt.Sprintf(
				rsyncCommand,
				deleteString,
				inputs[pxfBase],
				hostname)
		}, nil
	default:
		var effectivePxfBase string

		pxfCommand := ""
		if inputs[gpHome] != "" {
			pxfCommand += "GPHOME=" + inputs[gpHome] + " "
		}
		if inputs[pxfConf] != "" {
			pxfCommand += "PXF_CONF=" + inputs[pxfConf] + " "
		}
		if inputs[pxfBase] != "" {
			pxfCommand += "PXF_BASE=" + inputs[pxfBase] + " "
			effectivePxfBase = inputs[pxfBase]
		} else {
			// PXF_BASE defaults to PXF_HOME
			effectivePxfBase = inputs[pxfHome]
		}
		if inputs[javaHome] != "" {
			pxfCommand += "JAVA_HOME=" + inputs[javaHome] + " "
		}
		pxfCommand += inputs[pxfHome] + "/bin/pxf" + " " + string(cmd.name)
		if cmd.name == prepare && inputs[pxfHome] == effectivePxfBase {
			// error out when PXF_BASE equals PXF_HOME
			return nil, errors.New("the PXF_BASE value must be different from your PXF installation directory")
		}
		if cmd.name == migrate && inputs[pxfConf] == effectivePxfBase {
			// error out when PXF_BASE equals PXF_CONF
			return nil, errors.New("your target PXF_BASE directory must be different from your existing PXF_CONF directory")
		}
		if cmd.name == reset {
			pxfCommand += " --force" // there is a prompt for local reset as well
		}
		return func(_ string) string { return pxfCommand }, nil
	}
}

func (cmd *command) makeValidCliInputs() (map[envVar]string, error) {
	envVars := make(map[envVar]string, len(cmd.envVars))
	for _, e := range cmd.envVars {
		val, err := validateEnvVar(e)
		if err != nil {
			return nil, err
		}
		envVars[e] = val
	}

	return envVars, nil
}

func promptUser(input io.Reader, prompt string) bool {
	reader := bufio.NewReader(input)
	fmt.Print(prompt)
	text, _ := reader.ReadString('\n')
	text = strings.TrimRight(text, "\r\n")
	return strings.ToLower(text) == "y"
}

type commandName string

const (
	pxfInit  = "init"
	start    = "start"
	stop     = "stop"
	sync     = "sync"
	statuses = "status"
	reset    = "reset"
	register = "register"
	restart  = "restart"
	prepare  = "prepare"
	migrate  = "migrate"
)

// The pxf cli commands, exported for testing
var (
	InitCommand = command{
		name: pxfInit,
		messages: messages{
			success: "PXF initialized successfully on %d out of %d host%s\n",
			status: "*****************************************************************************\n" +
				"* DEPRECATION NOTICE:\n" +
				"* The \"pxf cluster init\" command is deprecated and will be removed\n" +
				"* in a future release of PXF.\n" +
				"*\n" +
				"* Use the \"pxf cluster register\" command instead.\n" +
				"*\n" +
				"*****************************************************************************\n\n" +
				"Initializing PXF on master host%s and %d segment host%s...\n",
			standby: ", standby master host,",
			err:     "PXF failed to initialize on %d out of %d host%s\n",
		},
		envVars:    []envVar{gpHome, pxfHome, javaHome},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	StartCommand = command{
		name: start,
		messages: messages{
			success: "PXF started successfully on %d out of %d host%s\n",
			status:  "Starting PXF on master host%s and %d segment host%s...\n",
			standby: ", standby master host,",
			err:     "PXF failed to start on %d out of %d host%s\n",
		},
		envVars:    []envVar{pxfHome, pxfBase},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	StopCommand = command{
		name: stop,
		messages: messages{
			success: "PXF stopped successfully on %d out of %d host%s\n",
			status:  "Stopping PXF on master host%s and %d segment host%s...\n",
			standby: ", standby master host,",
			err:     "PXF failed to stop on %d out of %d host%s\n",
		},
		envVars:    []envVar{pxfHome, pxfBase},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	SyncCommand = command{
		name: sync,
		messages: messages{
			success: "PXF configs synced successfully on %d out of %d host%s\n",
			status:  "Syncing PXF configuration files from master host to%s %d segment host%s...\n",
			standby: " standby master host and",
			err:     "PXF configs failed to sync on %d out of %d host%s\n",
		},
		envVars: []envVar{pxfBase},
		// cluster.ON_LOCAL | cluster.ON_HOSTS: the command will target host%s, but be run from master
		// this is ideal for copying files from master to segment host(s) using rsync.
		// since the files are already on master, we exclude master but include standby master
		whereToRun: cluster.ON_LOCAL | cluster.ON_HOSTS | cluster.EXCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	StatusCommand = command{
		name: statuses,
		messages: messages{
			success: "PXF is running on %d out of %d host%s\n",
			status:  "Checking status of PXF servers on master host%s and %d segment host%s...\n",
			standby: ", standby master host,",
			err:     "PXF is not running on %d out of %d host%s\n",
		},
		envVars:    []envVar{pxfHome, pxfBase},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	RegisterCommand = command{
		name: register,
		messages: messages{
			success: "PXF extension has been installed on %d out of %d host%s\n",
			status:  "Installing PXF extension on master host%s and %d segment host%s...\n",
			standby: ", standby master host,",
			err:     "Failed to install PXF extension on %d out of %d host%s\n",
		},
		envVars:    []envVar{gpHome, pxfHome},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	ResetCommand = command{
		name: reset,
		messages: messages{
			success: "PXF has been reset on %d out of %d host%s\n",
			status: "*****************************************************************************\n" +
				"* DEPRECATION NOTICE:\n" +
				"* The \"pxf cluster reset\" command is deprecated and will be removed\n" +
				"* in a future release of PXF.\n" +
				"*****************************************************************************\n\n" +
				"Resetting PXF on master host%s and %d segment host%s...\n",
			standby: ", standby master host,",
			err:     "Failed to reset PXF on %d out of %d host%s\n",
		},
		envVars:    []envVar{pxfHome},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	RestartCommand = command{
		name: restart,
		messages: messages{
			success: "PXF restarted successfully on %d out of %d host%s\n",
			status:  "Restarting PXF on master host%s and %d segment host%s...\n",
			standby: ", standby master host,",
			err:     "PXF failed to restart on %d out of %d host%s\n",
		},
		envVars:    []envVar{pxfHome, pxfBase},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	PrepareCommand = command{
		name: prepare,
		messages: messages{
			success: "PXF prepared successfully on %d out of %d host%s\n",
			status:  "Preparing PXF on master host%s and %d segment host%s...\n",
			standby: ", standby master host,",
			err:     "PXF failed to prepare on %d out of %d host%s\n",
		},
		envVars:    []envVar{pxfHome, pxfBase},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	MigrateCommand = command{
		name: migrate,
		messages: messages{
			success: "PXF configuration migrated successfully on %d out of %d host%s\n",
			status:  "Migrating PXF configuration on master host%s and %d segment host%s...\n",
			standby: ", standby master host,",
			err:     "PXF failed to migrate configuration on %d out of %d host%s\n",
		},
		envVars:    []envVar{pxfHome, pxfConf, pxfBase},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
)

func validateEnvVar(envVariable envVar) (string, error) {
	envVarValue, isEnvVarSet := os.LookupEnv(string(envVariable))
	if !isEnvVarSet {
		return "", errors.New(string(envVariable) + " must be set")
	}
	if envVarValue == "" {
		return "", errors.New(string(envVariable) + " cannot be blank")
	}
	return envVarValue, nil
}
