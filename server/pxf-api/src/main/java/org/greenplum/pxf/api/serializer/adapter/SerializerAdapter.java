package org.greenplum.pxf.api.serializer.adapter;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.time.LocalDateTime;

public interface SerializerAdapter {

    void open(final OutputStream out) throws IOException;

    void close(final OutputStream out) throws IOException;

    void startRow(OutputStream out, int numColumns) throws IOException;

    void startField(OutputStream out) throws IOException;

    void endField(OutputStream out) throws IOException;

    void endRow(OutputStream out) throws IOException;
    
    void writeNull(OutputStream out) throws IOException;

    void writeLong(OutputStream out, long value) throws IOException;

    void writeBoolean(OutputStream out, boolean value) throws IOException;

    void writeText(OutputStream out, String value) throws IOException;

    void writeText(OutputStream out, String value, Charset charset) throws IOException;

    void writeBytes(OutputStream out, byte[] value) throws IOException;

    void writeDouble(OutputStream out, double value) throws IOException;

    void writeInteger(OutputStream out, int value) throws IOException;

    void writeFloat(OutputStream out, float value) throws IOException;

    void writeShort(OutputStream out, short value) throws IOException;

    void writeDate(OutputStream out, Date date) throws IOException;

    void writeDate(OutputStream out, String date) throws IOException;

    void writeDate(OutputStream out, int date) throws IOException;

    void writeTimestamp(OutputStream out, String localDateTime) throws IOException;

    void writeTimestamp(OutputStream out, LocalDateTime localDateTime) throws IOException;

    void writeNumeric(OutputStream out, Number value) throws IOException;

    void writeNumeric(OutputStream out, String value) throws IOException;

    void writeNumeric(OutputStream out, BigDecimal value) throws IOException;
}
