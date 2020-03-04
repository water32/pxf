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
    private int currentImage = 0, numImages, w, h;
    private static final int INTENSITIES = 256, NUM_COL = 3;
    // cache of strings for RGB arrays going to Greenplum
    private static String[] r = new String[INTENSITIES];
    private static String[] g = new String[INTENSITIES];
    private static String[] b = new String[INTENSITIES];
    private DataType imageColumnType;
    private BufferedImage image;
    private boolean hasNext;

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
        getNextImage();
        w = image.getWidth();
        h = image.getHeight();
        LOG.debug("Image size {}w {}h", w, h);

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

    private void getNextImage() throws InterruptedException {
        image = accessor.next();
        currentImage++;
        hasNext = image != null;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    /**
     * Returns Postgres-style multi-dimensional array, piece by piece. Each
     * time this method is called it returns another image, where multiple images
     * will end up in the same tuple.
     */
    @Override
    public Object next() throws IOException, InterruptedException {
        if (image == null) {
            if (currentImage < numImages) {
                throw new IOException(
                        String.format("File %s yielded a null image, check contents", paths.get(currentImage))
                );
            }
            return null;
        }

        if (w != image.getWidth() || h != image.getHeight()) {
            throw new IOException(
                    String.format(
                            "Image from file %s has an inconsistent size %dx%d, should be %dx%d",
                            paths.get(currentImage),
                            image.getWidth(),
                            image.getHeight(),
                            w,
                            h
                    )
            );
        }

        if (imageColumnType == DataType.BYTEA) {
            byte[] imageByteArray = imageToByteArray(image, w, h);
            getNextImage();
            return imageByteArray;
        }

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

        getNextImage();
        return sb.toString();
    }

    private byte[] imageToByteArray(BufferedImage image, int w, int h) {
        byte[] bytea = new byte[w * h * NUM_COL];
        int cnt = 0;
        for (int pixel : image.getRGB(0, 0, w, h, null, 0, w)) {
            bytea[cnt++] = (byte) ((pixel >> 16) & 0xff);
            bytea[cnt++] = (byte) ((pixel >> 8) & 0xff);
            bytea[cnt++] = (byte) (pixel & 0xff);
        }
        return bytea;
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
