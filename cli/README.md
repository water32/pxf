# `pxf cluster` CLI

## Getting Started

1. Ensure you are set up for PXF development by following the README.md at the root of this repository. This tool requires Go version 1.21 or higher. Follow the directions [here](https://golang.org/doc/) to get the language set up.

1. Run the tests
   ```
   make test
   ```

1. Build the CLI
   ```
   make
   ```
   This will put the binary `pxf-cli` into `pxf/cli/build/`. You can also install the binary into `${PXF_HOME}/bin/pxf-cli` with:
   ```
   make install
   ```
1. Run some specific unit tests
   Make the `Describe` or `Context` to be _Focused_ by prepending an `F`, then run
   ```
   ginkgo
   ```
   See more in [Ginkgo documentation](https://onsi.github.io/ginkgo/#focused-specs).

1. There is also end to end testing for the pxf-cli located at `pxf/concourse/scripts/cli`. These tests can be run by flying the following pipeline:

   ```sh
   GP7_CLI=true make -C ~/workspace/pxf/concourse dev
   ```

## Debugging the CLI on a live system

Because it's hard to mock out a Greenplum cluster, it's useful to debug on a real live cluster. We can do this using the [`delve`](https://github.com/go-delve/delve) project.

1. Install `dlv` command, see [here](https://github.com/go-delve/delve/blob/master/Documentation/installation/linux/install.md) for more details:

```bash
go install github.com/go-delve/delve/cmd/dlv@latest
```

1. Optionally create a file with a list of commands, this could be useful to save you from re-running commands. You can actually run them with the `source` command in the `dlv` REPL:

```
config max-string-len 1000
break main.main
break cmd/cluster.go:24
continue
```

Inside the above `debug_command.txt`, I set a breakpoint in the main function, and another breakpoint inside the function `createCobraCommand`.
The `continue` command on the third line will navigate you to the first breakpoint.

1. Run the `dlv` command to enter the interactive REPL:

```bash
cd ~/workspace/pxf/cli
GPHOME=/usr/local/greenplum-db dlv debug pxf-cli -- cluster restart
```

The [help page for dlv](https://github.com/go-delve/delve/tree/master/Documentation/cli) is useful.

## Debugging the unit tests

Run the `dlv` command to enter the interactive REPL, and set the breakpoints of the tests:

```bash
cd ~/workspace/pxf/cli
dlv test
break cmd/cluster_test.go:613
continue
```

## Testing on a multi-node cluster manually

If we want to test cli in a real world Greenplum cluster, we can use the pipeline to spin up a multi-node cluster for testing.

1. In the pipeline, by default the GPDB cluster and the dataproc clusters are cleaned up after 3 hours.
If you want them to last longer, change the value of `ccp_reap_minutes` inside `test-multinode-tpl.yml` to be a longer time.

1. Fly the pipeline with the following command, the multinode el version can be 7, 8 or 9:

   ```bash
   MULTINODE_EL<version>=true make -C ~/workspace/pxf/concourse dev
   ```
1. When task `test-pxf-gp7-ext-hdp2-secure-multi-ipa-[rocky9|rocky8|centos7]` start to run the job `test-pxf-gp7-ext-hdp2-secure-multi-ipa-[rocky9|rocky8|centos7]`, enter the container.

1. If you want test things on coordinator, SSH into coordinator:

   ```bash
   $ ssh cdw
   # check you can connect to GPDB
   [gpadmin@cdw ~]$ source /usr/local/greenplum-db-<version>/greenplum_path.sh
   [gpadmin@cdw ~]$ psql
   psql (12.12)
   Type "help" for help.
   ```
   
   If you want to see segment configurations, run the following sql query:

   ```sql
   select * from gp_segment_configuration;
   ```

1. If you want to test things on segments, SSH to segment:

   ```bash
   ssh sdw1
   
   # connect to GPDB on the local segment machine
   [gpadmin@sdw1 ~]$ export PGPORT=20000
   [gpadmin@sdw1~]$ source /usr/local/greenplum-db-<version>/greenplum_path.sh
   # run psql with utility mode since this database instance is running as a primary segment in a Greenplum cluster
   # and does not permit direct connections.
   [gpadmin@sdw1~]$ PGOPTIONS='-c gp_session_role=utility' psql
   psql (12.12)
   Type "help" for help.
   
   # connect to GPDB on the coordinator
   [gpadmin@sdw1 ~]$ export PGHOST=cdw
   [gpadmin@sdw1 ~]$ export PGPORT=5432
   [gpadmin@sdw1 ~]$ source /usr/local/greenplum-db-<version>/greenplum_path.sh
   [gpadmin@sdw1 ~]$ psql
   psql (12.12)
   Type "help" for help.
   ```

## IDE Setup (GoLand)
* Start GoLand. Click "Open" and select the cli folder inside the pxf repo.
* Open preferences/settings and set under GO > GoPath, set Project GoPath to ~/workspace/pxf/cli/go or the equivalent.
* Under GO > Go Modules, select the enable button and leave the environment field empty.
* Check that it worked by running a test file (ex: go test pxf-cli/cmd)
