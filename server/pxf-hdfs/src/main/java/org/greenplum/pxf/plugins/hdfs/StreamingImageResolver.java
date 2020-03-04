package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.ArrayField;
import org.greenplum.pxf.api.ArrayStreamingField;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StreamingField;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.StreamingResolver;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This implementation of StreamingResolver works with the StreamingImageAccessor to
 * fetch and encode images into a string, one by one, placing multiple images into a single
 * field.
 * <p>
 * It hands off a reference to itself in a ArrayStreamingField so that the field can be used to
 * call back to the StreamingImageResolver in the BridgeOutputBuilder class. The resolver in turn
 * calls back to the StreamingImageAccessor to fetch images when needed.
 */
@SuppressWarnings("unchecked")
public class StreamingImageResolver extends BasePlugin implements StreamingResolver {
    private static final int IMAGE_DATA_COLUMN = 4;
    private StreamingImageAccessor accessor;
    private List<String> paths;
    private int currentImage = 0, currentThread = 0, numImages, w, h;
    private static final int INTENSITIES = 256, NUM_COL = 3;
    // cache of strings for RGB arrays going to Greenplum
    private static String[] r = new String[INTENSITIES];
    private static String[] g = new String[INTENSITIES];
    private static String[] b = new String[INTENSITIES];
    private DataType imageColumnType;
    private BufferedImage[] currentImages;
    private Thread[] threads;
    private Object[] imageArrays;

    static {
        String intStr;
        for (int i = 0; i < INTENSITIES; i++) {
            intStr = String.valueOf(i);
            r[i] = "{" + intStr;
            g[i] = "," + intStr + ",";
            b[i] = intStr + "}";
        }
    }

    /**
     * Returns Postgres-style arrays with full paths, parent directories, and names
     * of image files.
     */
    @Override
    public List<OneField> getFields(OneRow row) throws InterruptedException {
        imageColumnType = context.getColumn(IMAGE_DATA_COLUMN).getDataType();
        if (imageColumnType != DataType.INT2ARRAY && imageColumnType != DataType.INT4ARRAY &&
                imageColumnType != DataType.INT8ARRAY && imageColumnType != DataType.BYTEA) {
            throw new UnsupportedTypeException("image data column must be an integer or byte array");
        }
        paths = (ArrayList<String>) row.getKey();
        accessor = (StreamingImageAccessor) row.getData();
        List<String> fullPaths = new ArrayList<>();
        List<String> parentDirs = new ArrayList<>();
        List<String> fileNames = new ArrayList<>();

        for (String pathString : paths) {
            URI uri = URI.create(pathString);
            Path path = Paths.get(uri.getPath());

            fullPaths.add(uri.getPath());
            parentDirs.add(path.getParent().getFileName().toString());
            fileNames.add(path.getFileName().toString());
        }

        numImages = fileNames.size();
        // get the first image since we need the width and height early
        getNextImages();
        w = currentImages[0].getWidth();
        h = currentImages[0].getHeight();
        LOG.debug("Image size {}w {}h", w, h);

        currentThread = currentImages.length;
        threads = new Thread[currentImages.length];
        imageArrays = new Object[currentImages.length];

        return new ArrayList<OneField>() {
            {
                add(new ArrayField(DataType.TEXTARRAY.getOID(), fullPaths));
                add(new ArrayField(DataType.TEXTARRAY.getOID(), parentDirs));
                add(new ArrayField(DataType.TEXTARRAY.getOID(), fileNames));
                add(new ArrayField(DataType.INT8ARRAY.getOID(), new ArrayList<Integer>() {{
                    add(numImages);
                    add(h);
                    add(w);
                    add(NUM_COL);
                }}));
                if (imageColumnType == DataType.BYTEA) {
                    add(new StreamingField(DataType.BYTEA.getOID(), StreamingImageResolver.this));
                } else {
                    add(new ArrayStreamingField(StreamingImageResolver.this));
                }
            }
        };
    }

    private void getNextImages() throws InterruptedException {
        currentImages = accessor.next();
    }

    @Override
    public boolean hasNext() {
        return currentImage < numImages;
    }

    /**
     * Returns Postgres-style multi-dimensional int array or Postgres BYTEA, piece by piece. Each
     * time this method is called it returns another image, where multiple images
     * will end up in the same tuple.
     */
    @Override
    public Object next() throws InterruptedException {
        // if ((currentImages == null && currentThread == imageArrays.length) || (currentThread < currentImages.length && currentImages[currentThread] == null)) {
        //     if (currentImage < numImages) {
        //         throw new IOException(
        //                 String.format("File %s yielded a null image, check contents", paths.get(currentImage))
        //         );
        //     }
        //     return null;
        // }

        if (currentThread == imageArrays.length) {
            for (int i = 0; i < currentImages.length; i++) {
                threads[i] = new Thread(new ProcessImageRunnable(i));
                threads[i].start();
            }
            for (int i = 0; i < currentImages.length; i++) {
                threads[i].join();
            }
            currentThread = 0;
            getNextImages();
        }

        // checkCurrentImageSize();

        currentImage++;
        return imageArrays[currentThread++];
    }

    // private void checkCurrentImageSize() throws IOException {
    //     if (w != currentImages[currentThread].getWidth() || h != currentImages[currentThread].getHeight()) {
    //         throw new IOException(
    //                 String.format(
    //                         "Image from file %s has an inconsistent size %dx%d, should be %dx%d",
    //                         paths.get(currentImage),
    //                         currentImages[currentThread].getWidth(),
    //                         currentImages[currentThread].getHeight(),
    //                         w,
    //                         h
    //                 )
    //         );
    //     }
    // }

    class ProcessImageRunnable implements Runnable {
        private int cnt;

        public ProcessImageRunnable(int cnt) {
            this.cnt = cnt;
        }

        private byte[] imageToByteArray(BufferedImage image) {
            byte[] bytea = new byte[w * h * NUM_COL];
            int cnt = 0;
            for (int pixel : image.getRGB(0, 0, w, h, null, 0, w)) {
                bytea[cnt++] = (byte) ((pixel >> 16) & 0xff);
                bytea[cnt++] = (byte) ((pixel >> 8) & 0xff);
                bytea[cnt++] = (byte) (pixel & 0xff);
            }
            return bytea;
        }

        private String imageToPostgresArray(BufferedImage image) {
            StringBuilder sb;
            // avoid arrayCopy() in sb.append() by pre-calculating max image size
            sb = new StringBuilder(
                    w * h * 13 +  // each RGB is at most 13 chars: {255,255,255}
                            (w - 1) * h + // commas separating RGBs
                            h * 2 +       // curly braces surrounding each row of RGBs
                            h - 1 +       // commas separating each row
                            2             // outer curly braces for the image
            );
            LOG.debug("Image length: {}, cap: {}", sb.length(), sb.capacity());
            processImage(sb, image, w, h);
            LOG.debug("Image length: {}, cap: {}", sb.length(), sb.capacity());
            return sb.toString();
        }

        @Override
        public void run() {
            if (imageColumnType == DataType.BYTEA) imageArrays[cnt] = imageToByteArray(currentImages[cnt]);
            else imageArrays[cnt] = imageToPostgresArray(currentImages[cnt]);
        }
    }

    private static void processImage(StringBuilder sb, BufferedImage image, int w, int h) {
        if (image == null) {
            return;
        }

        sb.append("{{");
        int cnt = 0;
        for (int pixel : image.getRGB(0, 0, w, h, null, 0, w)) {
            sb.append(r[(pixel >> 16) & 0xff]).append(g[(pixel >> 8) & 0xff]).append(b[pixel & 0xff]).append(",");
            if (++cnt % w == 0) {
                sb.setLength(sb.length() - 1);
                sb.append("},{");
            }
        }
        sb.setLength(sb.length() - 2);
        sb.append("}");
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     */
    @Override
    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException();
    }
}
