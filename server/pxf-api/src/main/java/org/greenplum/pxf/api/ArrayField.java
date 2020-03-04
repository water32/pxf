package org.greenplum.pxf.api;

import java.util.List;

/**
 * A special case of OneField that represents an array.
 */
public class ArrayField extends OneField {
    private String separator = ",";

    /**
     * @param type Should be one of the DataType.*ARRAY types in the DataType class
     * @param val  A list of values that comprise the array
     */
    public ArrayField(int type, List<?> val) {
        super(type, val);
        this.setPrefix("{");
        this.setSuffix("}");
    }

    public String getSeparator() {
        return this.separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }
}
