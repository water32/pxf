package org.greenplum.pxf.automation.features.image;

import org.apache.commons.codec.binary.Hex;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HdfsReadableImageTest extends BaseFeature {
    private static final int NUM_IMAGES = 5;
    private String hdfsPath;
    private File[] imageFiles;
    private StringBuilder[] imagesPostgresArrays;
    private Table compareTable_intArray;
    private Table compareTable_byteArray;
    private ReadableExternalTable exTable_byteArray;
    private String[] fullPaths;
    private String[] parentDirectories;
    private String[] imageNames;
    private byte[][] imagesPostgresByteArray;
    private ProtocolEnum protocol;
    private int w = 256;
    private int h = 128;
    private final static int COLOR_MAX = 256;

    @Override
    public void beforeClass() throws Exception {
        super.beforeClass();
        protocol = ProtocolUtils.getProtocol();
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/readableImage";
        prepareData();
    }

    private void prepareData() throws Exception {
        BufferedImage[] bufferedImages = new BufferedImage[NUM_IMAGES];
        imageFiles = new File[NUM_IMAGES];
        fullPaths = new String[NUM_IMAGES];
        parentDirectories = new String[NUM_IMAGES];
        imageNames = new String[NUM_IMAGES];
        for (int i = 0; i < NUM_IMAGES; i++) {
            bufferedImages[i] = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        }
        imagesPostgresArrays = new StringBuilder[NUM_IMAGES];
        imagesPostgresByteArray = new byte[NUM_IMAGES][w * h * 3];
        appendToImages(imagesPostgresArrays, "{{", 0);

        Map<String, Integer> maskOffsets = new HashMap<String, Integer>() {{
            put("r", 16);
            put("g", 8);
            put("b", 0);
        }};

        Random rand = new Random();
        for (int j = 0; j < NUM_IMAGES; j++) {
            for (int i = 0; i < w * h; i++) {
                final int r = rand.nextInt(COLOR_MAX) << maskOffsets.get("r");
                final int g = rand.nextInt(COLOR_MAX) << maskOffsets.get("g");
                final int b = rand.nextInt(COLOR_MAX) << maskOffsets.get("b");
                bufferedImages[j].setRGB(i % w, i / w, r + g + b);
                imagesPostgresArrays[j]
                        .append("{")
                        .append(r >> maskOffsets.get("r"))
                        .append(",")
                        .append(g >> maskOffsets.get("g"))
                        .append(",")
                        .append(b >> maskOffsets.get("b"))
                        .append("},");
                imagesPostgresByteArray[j][i * 3] = (byte) (r >> maskOffsets.get("r"));
                imagesPostgresByteArray[j][i * 3 + 1] = (byte) (g >> maskOffsets.get("g"));
                imagesPostgresByteArray[j][i * 3 + 2] = (byte) (b >> maskOffsets.get("b"));
                if ((i + 1) % w == 0) {
                    appendToImage(imagesPostgresArrays[j], "},{", 1);
                }
            }
        }
        appendToImages(imagesPostgresArrays, "}", 2);

        String publicStage = "/tmp/publicstage/pxf";
        createDirectory(publicStage);

        int cnt = 0;
        for (BufferedImage bi : bufferedImages) {
            imageNames[cnt] = String.format("%d.png", cnt);
            imageFiles[cnt] = new File(publicStage + "/" + imageNames[cnt]);
            ImageIO.write(bi, "png", imageFiles[cnt]);
            fullPaths[cnt] = (protocol != ProtocolEnum.HDFS ? hdfsPath.replaceFirst("[^/]*/", "/") : "/" + hdfsPath) + "/" + imageNames[cnt];
            hdfs.copyFromLocal(imageFiles[cnt].toString(), hdfsPath + "/" + imageNames[cnt]);
            hdfs.copyFromLocal(imageFiles[cnt].toString(), hdfsPath + "_extra_dir/" + imageNames[cnt]);
            hdfs.copyFromLocal(imageFiles[cnt].toString(), hdfs.getWorkingDirectory() + "/streamingFragmentsReadableImage/dir1/" + imageNames[cnt]);
            hdfs.copyFromLocal(imageFiles[cnt].toString(), hdfs.getWorkingDirectory() + "/streamingFragmentsReadableImage/dir2/" + imageNames[cnt]);
            parentDirectories[cnt] = "readableImage";
            cnt++;
        }
    }

    private void createDirectory(String dir) {
        File publicStageDir = new File(dir);
        if (!publicStageDir.exists()) {
            if (!publicStageDir.mkdirs()) {
                throw new RuntimeException(String.format("Could not create %s", dir));
            }
        }
    }

    private void appendToImages(StringBuilder[] sbs, String s, int offsetFromEnd) {
        if (sbs[0] == null) {
            for (int i = 0; i < sbs.length; i++) {
                sbs[i] = new StringBuilder();
            }
        }
        for (StringBuilder sb : sbs) {
            appendToImage(sb, s, offsetFromEnd);
        }
    }

    private void appendToImage(StringBuilder sb, String s, int offsetFromEnd) {
        if (sb == null) {
            sb = new StringBuilder();
        }
        sb.setLength(sb.length() - offsetFromEnd);
        sb.append(s);
    }

    @Override
    public void beforeMethod() {
        // default external table with common settings (images become postgres int arrays)
        exTable = new ReadableExternalTable("image_test", null, "", "CSV");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        final String[] imageTableFields_intArray = new String[]{
                "fullpaths TEXT[]",
                "directories TEXT[]",
                "names TEXT[]",
                "dimensions INT[]",
                "images INT[]"
        };
        exTable.setFields(imageTableFields_intArray);
        exTable.setPath(hdfsPath + "/*.png");
        exTable.setProfile(protocol.value() + ":image");
        compareTable_intArray = new Table("compare_table_bytea", imageTableFields_intArray);
        compareTable_intArray.setRandomDistribution();
        // byte array table
        exTable_byteArray = new ReadableExternalTable("image_test_bytea", null, "", "CSV");
        exTable_byteArray.setHost(pxfHost);
        exTable_byteArray.setPort(pxfPort);
        final String[] imageTableFields_byteArray = new String[]{
                "fullpaths TEXT[]",
                "directories TEXT[]",
                "names TEXT[]",
                "dimensions INT[]",
                "images BYTEA"
        };
        exTable_byteArray.setFields(imageTableFields_byteArray);
        exTable_byteArray.setPath(hdfsPath + "/*.png");
        exTable_byteArray.setProfile(protocol.value() + ":image");
        compareTable_byteArray = new Table("compare_table_bytea", imageTableFields_byteArray);
        compareTable_byteArray.setRandomDistribution();
    }

    /**
     * Read a single image from HDFS
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void oneFilePerFragment() throws Exception {
        exTable.setName("image_test_one_file_per_fragment");
        compareTable_intArray.setName("compare_table_one_file_per_fragment");
        exTable_byteArray.setName("image_test_one_file_per_fragment_bytea");
        compareTable_byteArray.setName("compare_table_one_file_per_fragment_bytea");
        int cnt = 0;
        for (StringBuilder image : imagesPostgresArrays) {
            compareTable_intArray.addRow(new String[]{
                    "'{" + fullPaths[cnt] + "}'",
                    "'{" + parentDirectories[cnt] + "}'",
                    "'{" + imageNames[cnt] + "}'",
                    "'{" + 1 + "," + h + "," + w + ",3}'",
                    "'{" + image + "}'"
            });
            compareTable_byteArray.addRow(new String[]{
                    "'{" + fullPaths[cnt] + "}'",
                    "'{" + parentDirectories[cnt] + "}'",
                    "'{" + imageNames[cnt] + "}'",
                    "'{" + 1 + "," + h + "," + w + ",3}'",
                    "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[cnt]) + "'"
            });
            cnt++;
        }
        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable_intArray);
        gpdb.runQuery(compareTable_intArray.constructInsertStmt());
        gpdb.createTableAndVerify(exTable_byteArray);
        gpdb.createTableAndVerify(compareTable_byteArray);
        gpdb.runQuery(compareTable_byteArray.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.one_file_per_fragment.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void oneFilePerFragment_streamingFragments() throws Exception {
        exTable.setName("image_test_one_file_per_fragment_streaming_fragments");
        exTable.setUserParameters(new String[]{"STREAM_FRAGMENTS=true"});
        exTable.setPath(hdfs.getWorkingDirectory() + "/streamingFragmentsReadableImage");
        compareTable_intArray.setName("compare_table_one_file_per_fragment_streaming_fragments");
        exTable_byteArray.setName("image_test_one_file_per_fragment_streaming_fragments_bytea");
        exTable_byteArray.setUserParameters(new String[]{"STREAM_FRAGMENTS=true"});
        exTable_byteArray.setPath(hdfs.getWorkingDirectory() + "/streamingFragmentsReadableImage");
        compareTable_byteArray.setName("compare_table_one_file_per_fragment_streaming_fragments_bytea");
        int cnt = 0;
        for (StringBuilder image : imagesPostgresArrays) {
            compareTable_intArray.addRow(new String[]{
                    "'{" + fullPaths[cnt].replace("readableImage", "streamingFragmentsReadableImage/dir1") + "}'",
                    "'{dir1}'",
                    "'{" + imageNames[cnt] + "}'",
                    "'{" + 1 + "," + h + "," + w + ",3}'",
                    "'{" + image + "}'"
            });
            compareTable_intArray.addRow(new String[]{
                    "'{" + fullPaths[cnt].replace("readableImage", "streamingFragmentsReadableImage/dir2") + "}'",
                    "'{dir2}'",
                    "'{" + imageNames[cnt] + "}'",
                    "'{" + 1 + "," + h + "," + w + ",3}'",
                    "'{" + image + "}'"
            });
            compareTable_byteArray.addRow(new String[]{
                    "'{" + fullPaths[cnt].replace("readableImage", "streamingFragmentsReadableImage/dir1") + "}'",
                    "'{dir1}'",
                    "'{" + imageNames[cnt] + "}'",
                    "'{" + 1 + "," + h + "," + w + ",3}'",
                    "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[cnt]) + "'"
            });
            compareTable_byteArray.addRow(new String[]{
                    "'{" + fullPaths[cnt].replace("readableImage", "streamingFragmentsReadableImage/dir2") + "}'",
                    "'{dir2}'",
                    "'{" + imageNames[cnt] + "}'",
                    "'{" + 1 + "," + h + "," + w + ",3}'",
                    "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[cnt]) + "'"
            });
            cnt++;
        }
        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable_intArray);
        gpdb.runQuery(compareTable_intArray.constructInsertStmt());
        gpdb.createTableAndVerify(exTable_byteArray);
        gpdb.createTableAndVerify(compareTable_byteArray);
        gpdb.runQuery(compareTable_byteArray.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.one_file_per_fragment_streaming_fragments.runTest");
    }

    /**
     * Read a single image from HDFS
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void multiFragment() throws Exception {
        exTable.setName("image_test_multi_fragment");
        exTable.setUserParameters(new String[]{"FILES_PER_FRAGMENT=3"});
        compareTable_intArray.setName("compare_table_multi_fragment");
        compareTable_intArray.addRow(new String[]{
                "'{" + fullPaths[0] + "," + fullPaths[1] + "," + fullPaths[2] + "}'",
                "'{" + parentDirectories[0] + "," + parentDirectories[1] + "," + parentDirectories[2] + "}'",
                "'{" + imageNames[0] + "," + imageNames[1] + "," + imageNames[2] + "}'",
                "'{" + 3 + "," + h + "," + w + ",3}'",
                "'{" + imagesPostgresArrays[0] + "," + imagesPostgresArrays[1] + "," + imagesPostgresArrays[2] + "}'"
        });
        compareTable_intArray.addRow(new String[]{
                "'{" + fullPaths[3] + "," + fullPaths[4] + "}'",
                "'{" + parentDirectories[3] + "," + parentDirectories[4] + "}'",
                "'{" + imageNames[3] + "," + imageNames[4] + "}'",
                "'{" + 2 + "," + h + "," + w + ",3}'",
                "'{" + imagesPostgresArrays[3] + "," + imagesPostgresArrays[4] + "}'"
        });
        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable_intArray);
        gpdb.runQuery(compareTable_intArray.constructInsertStmt());

        exTable_byteArray.setName("image_test_multi_fragment_bytea");
        exTable_byteArray.setUserParameters(new String[]{"FILES_PER_FRAGMENT=3"});
        compareTable_byteArray.setName("compare_table_multi_fragment_bytea");
        compareTable_byteArray.addRow(new String[]{
                "'{" + fullPaths[0] + "," + fullPaths[1] + "," + fullPaths[2] + "}'",
                "'{" + parentDirectories[0] + "," + parentDirectories[1] + "," + parentDirectories[2] + "}'",
                "'{" + imageNames[0] + "," + imageNames[1] + "," + imageNames[2] + "}'",
                "'{" + 3 + "," + h + "," + w + ",3}'",
                "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[0]) + Hex.encodeHexString(imagesPostgresByteArray[1]) + Hex.encodeHexString(imagesPostgresByteArray[2]) + "'"
        });
        compareTable_byteArray.addRow(new String[]{
                "'{" + fullPaths[3] + "," + fullPaths[4] + "}'",
                "'{" + parentDirectories[3] + "," + parentDirectories[4] + "}'",
                "'{" + imageNames[3] + "," + imageNames[4] + "}'",
                "'{" + 2 + "," + h + "," + w + ",3}'",
                "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[3]) + Hex.encodeHexString(imagesPostgresByteArray[4]) + "'"
        });
        gpdb.createTableAndVerify(exTable_byteArray);
        gpdb.createTableAndVerify(compareTable_byteArray);
        gpdb.runQuery(compareTable_byteArray.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.multi_fragment.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void multiFragment_streamingFragments() throws Exception {
        exTable.setName("image_test_multi_fragment_streaming_fragments");
        exTable.setUserParameters(new String[]{"FILES_PER_FRAGMENT=3", "STREAM_FRAGMENTS=true"});
        exTable.setPath(hdfs.getWorkingDirectory() + "/streamingFragmentsReadableImage");
        compareTable_intArray.setName("compare_table_multi_fragment_streaming_fragments");
        compareTable_intArray.addRow(new String[]{
                "'{" + fullPaths[0].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[1].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[2].replace("readableImage", "streamingFragmentsReadableImage/dir1")
                        + "}'",
                "'{dir1,dir1,dir1}'",
                "'{" + imageNames[0] + "," + imageNames[1] + "," + imageNames[2] + "}'",
                "'{" + 3 + "," + h + "," + w + ",3}'",
                "'{" + imagesPostgresArrays[0] + "," + imagesPostgresArrays[1] + "," + imagesPostgresArrays[2] + "}'"
        });
        compareTable_intArray.addRow(new String[]{
                "'{" + fullPaths[3].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[4].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[0].replace("readableImage", "streamingFragmentsReadableImage/dir2") + "}'",
                "'{dir1,dir1,dir2}'",
                "'{" + imageNames[3] + "," + imageNames[4] + "," + imageNames[0] + "}'",
                "'{" + 3 + "," + h + "," + w + ",3}'",
                "'{" + imagesPostgresArrays[3] + "," + imagesPostgresArrays[4] + "," + imagesPostgresArrays[0] + "}'"
        });
        compareTable_intArray.addRow(new String[]{
                "'{" + fullPaths[1].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[2].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[3].replace("readableImage", "streamingFragmentsReadableImage/dir2") + "}'",
                "'{dir2,dir2,dir2}'",
                "'{" + imageNames[1] + "," + imageNames[2] + "," + imageNames[3] + "}'",
                "'{" + 3 + "," + h + "," + w + ",3}'",
                "'{" + imagesPostgresArrays[1] + "," + imagesPostgresArrays[2] + "," + imagesPostgresArrays[3] + "}'"
        });
        compareTable_intArray.addRow(new String[]{
                "'{" + fullPaths[4].replace("readableImage", "streamingFragmentsReadableImage/dir2") + "}'",
                "'{dir2}'",
                "'{" + imageNames[4] + "}'",
                "'{" + 1 + "," + h + "," + w + ",3}'",
                "'{" + imagesPostgresArrays[4] + "}'"
        });
        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable_intArray);
        gpdb.runQuery(compareTable_intArray.constructInsertStmt());

        exTable_byteArray.setName("image_test_multi_fragment_streaming_fragments_bytea");
        exTable_byteArray.setUserParameters(new String[]{"FILES_PER_FRAGMENT=3", "STREAM_FRAGMENTS=true"});
        exTable_byteArray.setPath(hdfs.getWorkingDirectory() + "/streamingFragmentsReadableImage");
        compareTable_byteArray.setName("compare_table_multi_fragment_streaming_fragments_bytea");
        compareTable_byteArray.addRow(new String[]{
                "'{" + fullPaths[0].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[1].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[2].replace("readableImage", "streamingFragmentsReadableImage/dir1")
                        + "}'",
                "'{dir1,dir1,dir1}'",
                "'{" + imageNames[0] + "," + imageNames[1] + "," + imageNames[2] + "}'",
                "'{" + 3 + "," + h + "," + w + ",3}'",
                "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[0]) +
                        Hex.encodeHexString(imagesPostgresByteArray[1]) +
                        Hex.encodeHexString(imagesPostgresByteArray[2]) + "'"
        });
        compareTable_byteArray.addRow(new String[]{
                "'{" + fullPaths[3].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[4].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[0].replace("readableImage", "streamingFragmentsReadableImage/dir2") + "}'",
                "'{dir1,dir1,dir2}'",
                "'{" + imageNames[3] + "," + imageNames[4] + "," + imageNames[0] + "}'",
                "'{" + 3 + "," + h + "," + w + ",3}'",
                "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[3]) +
                        Hex.encodeHexString(imagesPostgresByteArray[4]) +
                        Hex.encodeHexString(imagesPostgresByteArray[0]) + "'"
        });
        compareTable_byteArray.addRow(new String[]{
                "'{" + fullPaths[1].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[2].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[3].replace("readableImage", "streamingFragmentsReadableImage/dir2") + "}'",
                "'{dir2,dir2,dir2}'",
                "'{" + imageNames[1] + "," + imageNames[2] + "," + imageNames[3] + "}'",
                "'{" + 3 + "," + h + "," + w + ",3}'",
                "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[1]) +
                        Hex.encodeHexString(imagesPostgresByteArray[2]) +
                        Hex.encodeHexString(imagesPostgresByteArray[3]) + "'"
        });
        compareTable_byteArray.addRow(new String[]{
                "'{" + fullPaths[4].replace("readableImage", "streamingFragmentsReadableImage/dir2") + "}'",
                "'{dir2}'",
                "'{" + imageNames[4] + "}'",
                "'{" + 1 + "," + h + "," + w + ",3}'",
                "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[4]) + "'"
        });
        gpdb.createTableAndVerify(exTable_byteArray);
        gpdb.createTableAndVerify(compareTable_byteArray);
        gpdb.runQuery(compareTable_byteArray.constructInsertStmt());

        runTincTest("pxf.features.hdfs.readable.image.multi_fragment_streaming_fragments.runTest");
    }

    /**
     * Read a single image from HDFS
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void singleFragment() throws Exception {
        // all the files fit into a single fragment
        exTable.setName("image_test_single_fragment");
        exTable.setUserParameters(new String[]{"FILES_PER_FRAGMENT=10"});
        compareTable_intArray.setName("compare_table_single_fragment");
        compareTable_intArray.addRow(new String[]{
                "'{" + fullPaths[0] + "," + fullPaths[1] + "," + fullPaths[2] + "," + fullPaths[3] + "," + fullPaths[4] + "}'",
                "'{" + parentDirectories[0] + "," + parentDirectories[1] + "," + parentDirectories[2] + "," + parentDirectories[3] + "," + parentDirectories[4] + "}'",
                "'{" + imageNames[0] + "," + imageNames[1] + "," + imageNames[2] + "," + imageNames[3] + "," + imageNames[4] + "}'",
                "'{" + 5 + "," + h + "," + w + ",3}'",
                "'{" + imagesPostgresArrays[0] + ","
                        + imagesPostgresArrays[1] + ","
                        + imagesPostgresArrays[2] + ","
                        + imagesPostgresArrays[3] + ","
                        + imagesPostgresArrays[4] + "}'"
        });
        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable_intArray);
        gpdb.runQuery(compareTable_intArray.constructInsertStmt());

        exTable_byteArray.setName("image_test_single_fragment_bytea");
        exTable_byteArray.setUserParameters(new String[]{"FILES_PER_FRAGMENT=10"});
        compareTable_byteArray.setName("compare_table_single_fragment_bytea");
        compareTable_byteArray.addRow(new String[]{
                "'{" + fullPaths[0] + "," + fullPaths[1] + "," + fullPaths[2] + "," + fullPaths[3] + "," + fullPaths[4] + "}'",
                "'{" + parentDirectories[0] + "," + parentDirectories[1] + "," + parentDirectories[2] + "," + parentDirectories[3] + "," + parentDirectories[4] + "}'",
                "'{" + imageNames[0] + "," + imageNames[1] + "," + imageNames[2] + "," + imageNames[3] + "," + imageNames[4] + "}'",
                "'{" + 5 + "," + h + "," + w + ",3}'",
                "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[0]) +
                        Hex.encodeHexString(imagesPostgresByteArray[1]) +
                        Hex.encodeHexString(imagesPostgresByteArray[2]) +
                        Hex.encodeHexString(imagesPostgresByteArray[3]) +
                        Hex.encodeHexString(imagesPostgresByteArray[4]) + "'"

        });
        gpdb.createTableAndVerify(exTable_byteArray);
        gpdb.createTableAndVerify(compareTable_byteArray);
        gpdb.runQuery(compareTable_byteArray.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.single_fragment.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void singleFragment_streamingFragments() throws Exception {
        exTable.setName("image_test_single_fragment_streaming_fragments");
        exTable.setUserParameters(new String[]{"FILES_PER_FRAGMENT=10", "STREAM_FRAGMENTS=true"});
        exTable.setPath(hdfs.getWorkingDirectory() + "/streamingFragmentsReadableImage");
        compareTable_intArray.setName("compare_table_single_fragment_streaming_fragments");
        compareTable_intArray.addRow(new String[]{
                "'{" + fullPaths[0].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[1].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[2].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[3].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[4].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[0].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[1].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[2].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[3].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[4].replace("readableImage", "streamingFragmentsReadableImage/dir2") + "}'",
                "'{dir1,dir1,dir1,dir1,dir1,dir2,dir2,dir2,dir2,dir2}'",
                "'{" + imageNames[0] + "," + imageNames[1] + "," + imageNames[2] + "," + imageNames[3] + "," + imageNames[4] + ","
                        + imageNames[0] + "," + imageNames[1] + "," + imageNames[2] + "," + imageNames[3] + "," + imageNames[4] + "}'",
                "'{" + 10 + "," + h + "," + w + ",3}'",
                "'{" + imagesPostgresArrays[0] + ","
                        + imagesPostgresArrays[1] + ","
                        + imagesPostgresArrays[2] + ","
                        + imagesPostgresArrays[3] + ","
                        + imagesPostgresArrays[4] + ","
                        + imagesPostgresArrays[0] + ","
                        + imagesPostgresArrays[1] + ","
                        + imagesPostgresArrays[2] + ","
                        + imagesPostgresArrays[3] + ","
                        + imagesPostgresArrays[4] + "}'"
        });
        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable_intArray);
        gpdb.runQuery(compareTable_intArray.constructInsertStmt());

        exTable_byteArray.setName("image_test_single_fragment_streaming_fragments_bytea");
        exTable_byteArray.setUserParameters(new String[]{"FILES_PER_FRAGMENT=10", "STREAM_FRAGMENTS=true"});
        exTable_byteArray.setPath(hdfs.getWorkingDirectory() + "/streamingFragmentsReadableImage");
        compareTable_byteArray.setName("compare_table_single_fragment_streaming_fragments_bytea");
        compareTable_byteArray.addRow(new String[]{
                "'{" + fullPaths[0].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[1].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[2].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[3].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[4].replace("readableImage", "streamingFragmentsReadableImage/dir1") + ","
                        + fullPaths[0].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[1].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[2].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[3].replace("readableImage", "streamingFragmentsReadableImage/dir2") + ","
                        + fullPaths[4].replace("readableImage", "streamingFragmentsReadableImage/dir2") + "}'",
                "'{dir1,dir1,dir1,dir1,dir1,dir2,dir2,dir2,dir2,dir2}'",
                "'{" + imageNames[0] + "," + imageNames[1] + "," + imageNames[2] + "," + imageNames[3] + "," + imageNames[4] + ","
                        + imageNames[0] + "," + imageNames[1] + "," + imageNames[2] + "," + imageNames[3] + "," + imageNames[4] + "}'",
                "'{" + 10 + "," + h + "," + w + ",3}'",
                "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[0]) +
                        Hex.encodeHexString(imagesPostgresByteArray[1]) +
                        Hex.encodeHexString(imagesPostgresByteArray[2]) +
                        Hex.encodeHexString(imagesPostgresByteArray[3]) +
                        Hex.encodeHexString(imagesPostgresByteArray[4]) +
                        Hex.encodeHexString(imagesPostgresByteArray[0]) +
                        Hex.encodeHexString(imagesPostgresByteArray[1]) +
                        Hex.encodeHexString(imagesPostgresByteArray[2]) +
                        Hex.encodeHexString(imagesPostgresByteArray[3]) +
                        Hex.encodeHexString(imagesPostgresByteArray[4]) + "'"
        });
        gpdb.createTableAndVerify(exTable_byteArray);
        gpdb.createTableAndVerify(compareTable_byteArray);
        gpdb.runQuery(compareTable_byteArray.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.single_fragment_streaming_fragments.runTest");
    }

    /**
     * Read a single image from HDFS
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void filesInDifferentDirectories() throws Exception {
        // this test ensures that the different directories yield different
        // image 'labels'
        exTable.setName("image_test_images_in_different_directories");
        compareTable_intArray.setName("compare_table_images_in_different_directories");
        int cnt = 0;
        for (StringBuilder image : imagesPostgresArrays) {
            compareTable_intArray.addRow(new String[]{
                    "'{" + fullPaths[cnt] + "}'",
                    "'{" + parentDirectories[cnt] + "}'",
                    "'{" + imageNames[cnt] + "}'",
                    "'{" + 1 + "," + h + "," + w + ",3}'",
                    "'{" + image + "}'"
            });
            compareTable_intArray.addRow(new String[]{
                    "'{" + fullPaths[cnt].replace("readableImage", "readableImage_extra_dir") + "}'",
                    "'{" + parentDirectories[cnt] + "_extra_dir}'", // different directory reflected in image 'label'
                    "'{" + imageNames[cnt] + "}'",
                    "'{" + 1 + "," + h + "," + w + ",3}'",
                    "'{" + image + "}'"
            });
            cnt++;
        }
        exTable_byteArray.setName("image_test_images_in_different_directories_bytea");
        compareTable_byteArray.setName("compare_table_images_in_different_directories_bytea");
        cnt = 0;
        for (StringBuilder image : imagesPostgresArrays) {
            compareTable_byteArray.addRow(new String[]{
                    "'{" + fullPaths[cnt] + "}'",
                    "'{" + parentDirectories[cnt] + "}'",
                    "'{" + imageNames[cnt] + "}'",
                    "'{" + 1 + "," + h + "," + w + ",3}'",
                    "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[cnt]) + "'"
            });
            compareTable_byteArray.addRow(new String[]{
                    "'{" + fullPaths[cnt].replace("readableImage", "readableImage_extra_dir") + "}'",
                    "'{" + parentDirectories[cnt] + "_extra_dir}'", // different directory reflected in image 'label'
                    "'{" + imageNames[cnt] + "}'",
                    "'{" + 1 + "," + h + "," + w + ",3}'",
                    "'\\x" + Hex.encodeHexString(imagesPostgresByteArray[cnt]) + "'"
            });
            cnt++;
        }

        // open up path to include extra directory
        exTable.setPath(hdfsPath + "*/*.png");
        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable_intArray);
        gpdb.runQuery(compareTable_intArray.constructInsertStmt());
        exTable_byteArray.setPath(hdfsPath + "*/*.png");
        gpdb.createTableAndVerify(exTable_byteArray);
        gpdb.createTableAndVerify(compareTable_byteArray);
        gpdb.runQuery(compareTable_byteArray.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.images_in_different_directories.runTest");
    }


    @Override
    public void afterClass() throws Exception {
        super.afterMethod();
        if (ProtocolUtils.getPxfTestDebug().equals("true")) {
            return;
        }
        for (File fileToDelete : imageFiles) {
            if (!fileToDelete.delete()) {
                throw new RuntimeException(String.format("Could not delete %s", fileToDelete));
            }
        }
    }
}
