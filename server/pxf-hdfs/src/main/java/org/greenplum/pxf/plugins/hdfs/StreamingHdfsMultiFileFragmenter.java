package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.StreamingFragmenter;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class StreamingHdfsMultiFileFragmenter extends HdfsMultiFileFragmenter implements StreamingFragmenter {
    private List<String> files = new ArrayList<>();
    private List<Path> dirs = new ArrayList<>();
    private int currentDir = 0;
    private int currentFile = 0;
    private FileSystem fs;

    @Override
    public List<Path> getDirs() {
        return dirs;
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
     * Not implemented, see StreamingHdfsMultiFileFragmenter#next() and StreamingHdfsMultiFileFragmenter#hasNext()
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
