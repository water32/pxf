package org.greenplum.pxf.plugins.hdfs;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Accessor that can access multiple image files, treating the collection of images
 * as a single row. It maintains a list of InputStreams which it opens in openForRead(),
 * It hands off a reference to itself so that the StreamingImageResolver can fetch more images.
 */
public class StreamingImageAccessor extends BasePlugin implements Accessor {
    private List<String> paths;
    private FileSplit fileSplit;
    private FileSystem fs;
    private boolean served = false;
    int currentPath = 0;
    private int NUM_THREADS;
    private BufferedImage[] currentImages;
    private Thread[] threads;
    // private int currentImage;
    private static final String CONCURRENT_IMAGE_READS_OPTION = "ACCESSOR_THREADS";

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        NUM_THREADS = Integer.parseInt(context.getOption(CONCURRENT_IMAGE_READS_OPTION, "1"));
        currentImages = new BufferedImage[NUM_THREADS];
        threads = new Thread[NUM_THREADS];
        // currentImage = NUM_THREADS;
        fileSplit = HdfsUtilities.parseFileSplit(context);
    }

    /**
     * Opens the file using the non-splittable API for HADOOP HDFS file access
     * This means that instead of using a FileInputFormat for access, we use a
     * Java stream.
     *
     * @return true for successful file open, false otherwise
     */
    @Override
    public boolean openForRead() throws Exception {
        if (!isWorkingSegment()) {
            return false;
        }

        // input data stream, FileSystem.get actually
        // returns an FSDataInputStream
        paths = new ArrayList<>(Arrays.asList(context.getDataSource().split(",")));
        fs = FileSystem.get(URI.create(paths.get(0)), configuration);

        return paths.size() > 0;
    }

    /**
     * Closes the access stream when finished reading the file
     */
    @Override
    public void closeForRead() {
    }

    @Override
    public OneRow readNextObject() {
        /* check if working segment */
        if (served) {
            return null;
        }

        served = true;
        return new OneRow(paths, this);
    }

    public class FetchImageRunnable implements Runnable {
        private int cnt;

        public FetchImageRunnable(int cnt) {
            this.cnt = cnt;
        }

        @Override
        public void run() {
            try (InputStream stream = fs.open(new Path(paths.get(currentPath + cnt)))) {
                currentImages[cnt] = ImageIO.read(stream);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Caught an IOException reading image in thread %d", cnt), e);
            }
        }
    }

    public BufferedImage[] next() throws InterruptedException {
        if (currentPath == paths.size()) {
            return null;
        }
        // long now;
        // if (currentImage == NUM_THREADS) {
        // if (LOG.isDebugEnabled()) {
        //     now = System.currentTimeMillis();
        //     if (numThreads > 0) {
        //         LOG.debug("---> {} images were resolved in {} ms", numThreads, now - then);
        //     }
        //     then = now;
        // }
        // private long then = System.currentTimeMillis();
        int numThreads = currentPath + NUM_THREADS <= paths.size() ? NUM_THREADS : paths.size() - currentPath;
        currentImages = new BufferedImage[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new FetchImageRunnable(i));
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
        // if (LOG.isDebugEnabled()) {
        //     now = System.currentTimeMillis();
        //     LOG.debug("---> {} images were fetched in {} ms", numThreads, now - then);
        //     then = now;
        // }
        // currentImage = 0;
        // }
        // currentPath++;
        currentPath += numThreads;
        return currentImages;
    }

    public boolean hasNext() {
        return currentPath < paths.size();
    }

    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException();
    }

    /*
     * Making sure that only the segment that got assigned the first data
     * fragment will read the (whole) file.
     */
    private boolean isWorkingSegment() {
        return (fileSplit.getStart() == 0L);
    }

    @Override
    public boolean isThreadSafe() {
        return HdfsUtilities.isThreadSafe(
                configuration,
                context.getDataSource(),
                context.getOption("COMPRESSION_CODEC"));
    }

    public List<String> getPaths() {
        return paths;
    }
}
