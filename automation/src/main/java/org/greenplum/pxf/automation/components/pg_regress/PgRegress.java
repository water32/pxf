package org.greenplum.pxf.automation.components.pg_regress;

import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

import java.io.File;
import java.io.IOException;
import java.util.StringJoiner;

public class PgRegress extends ShellSystemObject {
    private String dbName;
    private String pgRegress;
    private String regressTestFolder;

    @Override
    public void init() throws Exception {
        ReportUtils.startLevel(report, getClass(), "init");
        super.init();
        runCommand("source $GPHOME/greenplum_path.sh");

        // logic for dynamically finding pg_regress path
        runCommand("readlink --canonicalize-existing \"$(dirname $(pg_config --pgxs))/../test/regress/pg_regress\"");
        String lastCmdResult = getLastCmdResult();
        String[] tokens = lastCmdResult.split("\r\n", 3);
        pgRegress = tokens[1];

        runCommand("cd " + new File(regressTestFolder).getAbsolutePath());
        ReportUtils.stopLevel(report);
    }

    public void runPgRegress(String pgRegressTestPath, String ... tests) throws IOException, ShellCommandErrorException {
        ReportUtils.startLevel(report, getClass(), "Run Test: " + pgRegressTestPath);

        setCommandTimeout(_10_MINUTES);
        StringJoiner commandToRun = new StringJoiner(" ");

        commandToRun.add(pgRegress);
        commandToRun.add("--use-existing");
        commandToRun.add("--inputdir=" + pgRegressTestPath);
        commandToRun.add("--outputdir=" + pgRegressTestPath);
        commandToRun.add("--dbname=" + dbName);

        for (String test : tests) {
            commandToRun.add(test);
        }

        runCommand(commandToRun.toString());

        ReportUtils.stopLevel(report);
    }
    public void runTest(String pgRegressTestPath, String ... tests) throws IOException, ShellCommandErrorException {
        runPgRegress(pgRegressTestPath, tests);
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getRegressTestFolder() {
        return regressTestFolder;
    }

    public void setRegressTestFolder(String regressTestFolder) {
        this.regressTestFolder = regressTestFolder;
    }
}
