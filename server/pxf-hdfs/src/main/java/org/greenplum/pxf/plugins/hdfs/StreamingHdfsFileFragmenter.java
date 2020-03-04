package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.StreamingFragmenter;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class StreamingHdfsFileFragmenter extends BaseFragmenter implements StreamingFragmenter {
    public final static String FILES_PER_FRAGMENT_OPTION_NAME = "FILES_PER_FRAGMENT";
    private int filesPerFragment;
    private List<String> files = new ArrayList<>();
    private List<Path> dirs = new ArrayList<>();
    private int currentDir = 0;
    private int currentFile = 0;

    @Override
    public List<Path> getDirs() {
        return dirs;
    }

    HcfsType hcfsType;
    Configuration jobConf;
    FileSystem fs;

    public int getFilesPerFragment() {
        return filesPerFragment;
    }

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(configuration, context);
        jobConf = new JobConf(configuration, this.getClass());

        filesPerFragment = 1;
        final String filesPerFragmentOptionString = context.getOption(FILES_PER_FRAGMENT_OPTION_NAME);
        if (filesPerFragmentOptionString != null) {
            filesPerFragment = Integer.parseInt(filesPerFragmentOptionString);
        }
    }

    @Override
    public void open() throws Exception {
        if (!dirs.isEmpty()) {
            return;
        }
        String path = hcfsType.getDataUri(jobConf, context);
        fs = FileSystem.get(new URI(path), jobConf);
        getDirs(new Path(path));
        dirs.sort(Comparator.comparing(Path::toString));
    }

    /**
     * Gets a batch of files and returns it as a comma-separated list within a single Fragment.
     */
    @Override
    public Fragment next() throws IOException {
        StringBuilder pathList = new StringBuilder();
        for (int i = 0; i < filesPerFragment; i++) {
            if (currentFile == files.size()) {
                getMoreFiles();
                if (currentFile == files.size() && currentDir == dirs.size()) {
                    break;
                }
            }
            pathList.append(files.set(currentFile++, null)).append(",");
        }
        if (pathList.length() == 0) {
            return null;
        }
        pathList.setLength(pathList.length() - 1);

        return new Fragment(pathList.toString());
    }

    @Override
    public boolean hasNext() throws IOException {
        if (currentFile < files.size()) {
            return true;
        }
        if (currentDir < dirs.size()) {
            getMoreFiles();
        }
        return files.size() > 0 && currentFile < files.size();
    }

    private void getMoreFiles() throws IOException {
        currentFile = 0;
        files.clear();
        while (currentDir < dirs.size() && files.isEmpty()) {
            final Path dir = dirs.get(currentDir++);
            files = Arrays
                    .stream(fs.listStatus(dir))
                    .filter(file -> !file.isDirectory())
                    .map(file -> file.getPath().toUri().toString())
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private void getDirs(Path path) throws IOException {
        dirs.add(path);
        RemoteIterator<FileStatus> iterator = fs.listStatusIterator(path); // no recursion
        while (iterator.hasNext()) {
            FileStatus fileStatus = iterator.next();
            if (fileStatus.isDirectory()) {
                Path curPath = fileStatus.getPath();
                getDirs(curPath);
            }
        }
    }

    /**
     * Not implemented, see StreamingHdfsFileFragmenter#next() and StreamingHdfsFileFragmenter#hasNext()
     */
    @Override
    public List<Fragment> getFragments() {
        throw new UnsupportedOperationException("Operation getFragments is not supported, use next() and hasNext()");
    }

    @Override
    public FragmentStats getFragmentStats() {
        throw new UnsupportedOperationException("Operation getFragmentStats is not supported");
    }
}
