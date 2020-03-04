package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.ArrayField;
import org.greenplum.pxf.api.ArrayStreamingField;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
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
    StreamingImageAccessor accessor;
    List<String> paths;
    int currentImage = 0;
    // cache of strings for RGB arrays going to Greenplum
    private static String[] r = new String[256];
    private static String[] g = new String[256];
    private static String[] b = new String[256];

    static {
        String intStr;
        for (int i = 0; i < 256; i++) {
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
    public List<OneField> getFields(OneRow row) {
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

        return new ArrayList<OneField>() {
            {
                add(new ArrayField(DataType.TEXTARRAY.getOID(), fullPaths));
                add(new ArrayField(DataType.TEXTARRAY.getOID(), parentDirs));
                add(new ArrayField(DataType.TEXTARRAY.getOID(), fileNames));
                add(new ArrayStreamingField(StreamingImageResolver.this));
            }
        };
    }

    @Override
    public boolean hasNext() {
        return accessor.hasNext();
    }

    /**
     * Returns Postgres-style multi-dimensional array, piece by piece. Each
     * time this method is called it returns another image, where multiple images
     * will end up in the same tuple.
     */
    @Override
    public String next() throws IOException {
        currentImage++;
        BufferedImage image = accessor.next();
        if (image == null) {
            if (currentImage < paths.size()) {
                throw new IOException("File " + paths.get(currentImage) + " yielded a null image, check contents");
            }
            return null;
        }

        StringBuilder sb;
        int w = image.getWidth();
        int h = image.getHeight();
        LOG.debug("Image size {}w {}h", w, h);
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
