package org.greenplum.pxf.api.serializer.adapter;

import org.greenplum.pxf.api.GreenplumDateTime;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

@Component
public class BinarySerializerAdapter implements SerializerAdapter {

    private static final int DECIMAL_DIGITS = 4;

    private static final BigInteger TEN = new BigInteger("10");
    private static final BigInteger TEN_THOUSAND = new BigInteger("10000");

    private static final LocalDateTime JAVA_EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
    private static final LocalDateTime POSTGRES_EPOCH = LocalDateTime.of(2000, 1, 1, 0, 0, 0);
    private static final long DAYS_BETWEEN_JAVA_AND_POSTGRES_EPOCHS = ChronoUnit.DAYS.between(JAVA_EPOCH, POSTGRES_EPOCH);

    private static final String BINARY_FORMAT_HEADER = "PXF\n\377\r\n\0";

    @Override
    public void open(OutputStream out) throws IOException {
        // 8 bytes required header
        int len = BINARY_FORMAT_HEADER.length();
        for (int i = 0; i < len; i++) {
            out.write((byte) BINARY_FORMAT_HEADER.charAt(i));
        }
        // 32 bit integer indicating no OID
        writeIntInternal(out, 0);
        // 32 bit header extension area length
        writeIntInternal(out, 0);
    }

    @Override
    public void startRow(OutputStream out, int numColumns) throws IOException {
        // Each tuple begins with a 16-bit integer count of the number of
        // fields in the tuple. In the PXF binary format, we write a single
        // byte where:
        //  - byte 0x01 represents EOF
        //  - byte 0x02 represents data
        //  - byte 0x03 represents error
        out.write(0x02);
    }

    @Override
    public void startField(OutputStream out) {
    } // DO NOTHING

    @Override
    public void endField(OutputStream out) {
    } // DO NOTHING

    @Override
    public void endRow(OutputStream out) {
    } // DO NOTHING

    @Override
    public void close(OutputStream out) throws IOException {
        // The file trailer consists of a single byte containing byte 0x01.
        out.write(0x01);
        out.flush();
    }

    @Override
    public void writeNull(OutputStream out) throws IOException {
        // As a special case, -1 indicates a NULL field value.
        // No value bytes follow in the NULL case.
        writeIntInternal(out, -1);
    }

    @Override
    public void writeLong(OutputStream out, long value) throws IOException {
        // Write the length=8 of the field
        writeIntInternal(out, 8);
        // Write the value of the field
        writeLongInternal(out, value);
    }

    @Override
    public void writeBoolean(OutputStream out, boolean value) throws IOException {
        // Write the length=1 of the field
        writeIntInternal(out, 1);
        // Write the value of the field
        out.write(value ? 1 : 0);
    }

    @Override
    public void writeText(OutputStream out, String value) throws IOException {
        writeText(out, value, StandardCharsets.UTF_8);
    }

    @Override
    public void writeText(OutputStream out, String value, Charset charset) throws IOException {
        writeBytes(out, value.getBytes(charset));
    }

    @Override
    public void writeBytes(OutputStream out, byte[] value) throws IOException {
        // Write the length of the field
        writeIntInternal(out, value.length);
        // Write the value of the field
        out.write(value, 0, value.length);
    }

    @Override
    public void writeDouble(OutputStream out, double value) throws IOException {
        // Write the length=8 of the field
        writeIntInternal(out, 8);
        // Write the value of the field
        writeLongInternal(out, Double.doubleToLongBits(value));
    }

    @Override
    public void writeInteger(OutputStream out, int value) throws IOException {
        // Write the length=4 of the field
        writeIntInternal(out, 4);
        // Write the value of the field
        writeIntInternal(out, value);
    }

    @Override
    public void writeFloat(OutputStream out, float value) throws IOException {
        // Write the length=4 of the field
        writeIntInternal(out, 4);
        // Write the value of the field
        writeIntInternal(out, Float.floatToIntBits(value));
    }

    @Override
    public void writeShort(OutputStream out, short value) throws IOException {
        // Write the length=2 of the field
        writeIntInternal(out, 2);
        // Write the value of the field
        writeShortInternal(out, value);
    }

    @Override
    public void writeDate(OutputStream out, Date date) throws IOException {
//        doWriteDate(out, dateConverter.convert(((Date) value).toLocalDate());
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDate(OutputStream out, String date) throws IOException {
//        doWriteDate(out, dateConverter.convert(LocalDate.parse(value.toString())));
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDate(OutputStream out, int date) throws IOException {
        // Write the length=4 of the field
        writeIntInternal(out, 4);
        // Write the value of the field
        writeIntInternal(out, date);
    }

    @Override
    public void writeTimestamp(OutputStream out, String localDateTime) throws IOException {
        writeTimestamp(out, LocalDateTime.parse(localDateTime, GreenplumDateTime.DATETIME_FORMATTER));
    }

    @Override
    public void writeTimestamp(OutputStream out, LocalDateTime localDateTime) throws IOException {
        // Write the length=8 of the field
        writeIntInternal(out, 8);
        // Write the value of the field
        writeLongInternal(out, convertLocalDateTime(localDateTime));
    }

    @Override
    public void writeNumeric(OutputStream out, Number value) throws IOException {
        if (value instanceof BigDecimal) {
            writeNumeric(out, (BigDecimal) value);
        } else {
            writeNumeric(out, Double.toString(value.doubleValue()));
        }
    }

    @Override
    public void writeNumeric(OutputStream out, String value) throws IOException {
        int len = value.length();
        // Write the length of the field
        writeIntInternal(out, len);
        // Write the value of the field
        for (int i = 0; i < len; i++) {
            out.write((byte) value.charAt(i));
        }
    }

    /**
     * The Algorithm for turning a BigDecimal into a Postgres Numeric is
     * heavily inspired by the Intermine Implementation:
     * <p>
     * https://github.com/intermine/intermine/blob/dev/intermine/objectstore/src/main/java/org/intermine/sql/writebatch/BatchWriterPostgresCopyImpl.java
     */
    @Override
    public void writeNumeric(OutputStream out, BigDecimal value) throws IOException {
        writeNumeric(out, value.toString());

//        BigInteger unscaledValue = value.unscaledValue();
//
//        int sign = value.signum();
//
//        if (sign == -1) {
//            unscaledValue = unscaledValue.negate();
//        }
//
//        // Number of fractional digits:
//        int fractionDigits = value.scale();
//
//        // Number of Fraction Groups:
//        int fractionGroups = (fractionDigits + 3) / 4;
//
//        List<Integer> digits = new ArrayList<>();
//
//        // The scale needs to be a multiple of 4:
//        int scaleRemainder = fractionDigits % 4;
//
//        // Scale the first value:
//        if (scaleRemainder != 0) {
//            BigInteger[] result = unscaledValue.divideAndRemainder(TEN.pow(scaleRemainder));
//
//            int digit = result[1].intValue() * (int) Math.pow(10, DECIMAL_DIGITS - scaleRemainder);
//
//            digits.add(digit);
//
//            unscaledValue = result[0];
//        }
//
//        while (!unscaledValue.equals(BigInteger.ZERO)) {
//            BigInteger[] result = unscaledValue.divideAndRemainder(TEN_THOUSAND);
//            digits.add(result[1].intValue());
//            unscaledValue = result[0];
//        }
//
//        writeIntInternal(out, 8 + (2 * digits.size()));
//        writeShortInternal(out, digits.size());
//        writeShortInternal(out, digits.size() - fractionGroups - 1);
//        writeShortInternal(out, sign == 1 ? 0x0000 : 0x4000);
//        writeShortInternal(out, fractionDigits);
//
//        // Now write each digit:
//        for (int pos = digits.size() - 1; pos >= 0; pos--) {
//            writeShortInternal(out, digits.get(pos));
//        }
    }

    /**
     * Writes an <code>int</code> to the <code>out</code> stream as four
     * bytes, high byte first.
     *
     * @param out the output stream where <code>v</code> will be written.
     * @param v   an <code>int</code> to be written.
     * @throws IOException if an I/O error occurs.
     */
    private void writeIntInternal(OutputStream out, int v) throws IOException {

        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    /**
     * Writes a <code>short</code> to the <code>out</code> stream as four
     * * bytes, high byte first.
     *
     * @param out the output stream where <code>v</code> will be written.
     * @param v   a <code>short</code> to be written.
     * @throws IOException if an I/O error occurs.
     */
    public void writeShortInternal(OutputStream out, int v) throws IOException {
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    /**
     * Writes a <code>long</code> to the <code>out</code> stream as eight
     * bytes, high byte first.
     *
     * @param out the output stream where <code>v</code> will be written.
     * @param v   a <code>long</code> to be written.
     * @throws IOException if an I/O error occurs.
     */
    private void writeLongInternal(OutputStream out, long v) throws IOException {
        out.write((byte) (v >>> 56));
        out.write((byte) (v >>> 48));
        out.write((byte) (v >>> 40));
        out.write((byte) (v >>> 32));
        out.write((byte) (v >>> 24));
        out.write((byte) (v >>> 16));
        out.write((byte) (v >>> 8));
        out.write((byte) v);
    }

    private long convertLocalDateTime(LocalDateTime dateTime) {
        // Extract the Time of the Day in Nanoseconds:
        long timeInNanoseconds = dateTime
                .toLocalTime()
                .toNanoOfDay();

        // Convert the Nanoseconds to Microseconds:
        long timeInMicroseconds = timeInNanoseconds / 1000;

        // Now Calculate the Postgres Timestamp:
        if (dateTime.isBefore(POSTGRES_EPOCH)) {
            long dateInMicroseconds = (dateTime.toLocalDate().toEpochDay() - DAYS_BETWEEN_JAVA_AND_POSTGRES_EPOCHS) * 86400000000L;

            return dateInMicroseconds + timeInMicroseconds;
        } else {
            long dateInMicroseconds = (DAYS_BETWEEN_JAVA_AND_POSTGRES_EPOCHS - dateTime.toLocalDate().toEpochDay()) * 86400000000L;

            return -(dateInMicroseconds - timeInMicroseconds);
        }
    }
}
