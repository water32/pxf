package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.GreenplumDateTime;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public abstract class BinaryTupleSerializer<T> extends BaseTupleSerializer<T> {

    private static final String BINARY_FORMAT_HEADER = "PGCOPY\n\377\r\n\0";

    private static final int DECIMAL_DIGITS = 4;

    private static final BigInteger TEN = new BigInteger("10");
    private static final BigInteger TEN_THOUSAND = new BigInteger("10000");

    @Override
    public void open(DataOutputStream out) throws IOException {
        // 11 bytes required header
        out.writeBytes("PGCOPY\n\377\r\n\0");
        // 32 bit integer indicating no OID
        out.writeInt(0);
        // 32 bit header extension area length
        out.writeInt(0);
    }

    @Override
    public void startRow(DataOutputStream out, int numColumns) throws IOException {
        // Each tuple begins with a 16-bit integer count of the number of
        // fields in the tuple
        out.write((numColumns >>> 8) & 0xFF);
        out.write((numColumns >>> 0) & 0xFF);
    }

    @Override
    public void startField(DataOutputStream out) throws IOException {
    } // DO NOTHING

    @Override
    public void endField(DataOutputStream out) throws IOException {
    } // DO NOTHING

    @Override
    public void endRow(DataOutputStream out) throws IOException {
    } // DO NOTHING

    @Override
    public void close(DataOutputStream out) throws IOException {
        // The file trailer consists of a 16-bit integer word containing -1.
        // This is easily distinguished from a tuple's field-count word.
        out.writeShort(-1);
    }

    protected void writeNull(DataOutputStream out) throws IOException {
        // As a special case, -1 indicates a NULL field value.
        // No value bytes follow in the NULL case.
        out.writeInt(-1);
    }

    protected void doWriteLong(DataOutputStream out, long value) throws IOException {
        // Write the length=8 of the field
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(8);
        // Write the value of the field
        out.writeLong(value);
    }

    protected void doWriteBoolean(DataOutputStream out, boolean value) throws IOException {
        // Write the length=1 of the field
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(1);
        // Write the value of the field
        out.writeByte(value ? 1 : 0);
    }

    protected void doWriteText(DataOutputStream out, String value) throws IOException {
        doWriteText(out, value, StandardCharsets.UTF_8);
    }

    protected void doWriteText(DataOutputStream out, String value, Charset charset) throws IOException {
        doWriteBytes(out, value.getBytes(charset));
    }

    protected void doWriteBytes(DataOutputStream out, byte[] value) throws IOException {
        int length = value.length;
        // Write the length of the field
        out.write((length >>> 24) & 0xFF);
        out.write((length >>> 16) & 0xFF);
        out.write((length >>> 8) & 0xFF);
        out.write((length >>> 0) & 0xFF);
        // Write the value of the field
        out.write(value, 0, length);
    }

    protected void doWriteDouble(DataOutputStream out, double value) throws IOException {
        // Write the length=8 of the field
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(8);
        // Write the value of the field
        out.writeDouble(value);
    }

    protected void doWriteInteger(DataOutputStream out, int value) throws IOException {
        // Write the length=4 of the field
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(4);
        // Write the value of the field
        out.writeInt(value);
    }

    protected void doWriteFloat(DataOutputStream out, float value) throws IOException {
        // Write the length=4 of the field
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(4);
        // Write the value of the field
        out.writeFloat(value);
    }

    protected void doWriteShort(DataOutputStream out, short value) throws IOException {
        // Write the length=2 of the field
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(2);
        // Write the value of the field
        out.writeShort(value);
    }

    protected void doWriteDate(DataOutputStream out, Date date) throws IOException {
//        doWriteDate(out, dateConverter.convert(((Date) value).toLocalDate());
        throw new UnsupportedOperationException();
    }

    protected void doWriteDate(DataOutputStream out, String date) throws IOException {
//        doWriteDate(out, dateConverter.convert(LocalDate.parse(value.toString())));
        throw new UnsupportedOperationException();
    }

    protected void doWriteDate(DataOutputStream out, int date) throws IOException {
        // Write the length=4 of the field
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(4);
        // Write the value of the field
        out.write((date >>> 24) & 0xFF);
        out.write((date >>> 16) & 0xFF);
        out.write((date >>> 8) & 0xFF);
        out.write((date >>> 0) & 0xFF);
    }

    protected void doWriteTimestamp(DataOutputStream out, String localDateTime) throws IOException {
        doWriteTimestamp(out, LocalDateTime.parse(localDateTime, GreenplumDateTime.DATETIME_FORMATTER));
    }

    protected void doWriteTimestamp(DataOutputStream out, LocalDateTime localDateTime) throws IOException {
        // Write the length=8 of the field
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(8);
        // Write the value of the field
//        out.writeLong(dateTimeConverter.convert(localDateTime));
        throw new UnsupportedOperationException();
    }

    protected void doWriteNumeric(DataOutputStream out, Number value) throws IOException {
        if (value instanceof BigDecimal) {
            doWriteNumeric(out, (BigDecimal) value);
        } else {
            doWriteNumeric(out, new BigDecimal(Double.toString(value.doubleValue())));
        }
    }

    protected void doWriteNumeric(DataOutputStream out, String value) throws IOException {
        doWriteNumeric(out, new BigDecimal(value));
    }

    /**
     * The Algorithm for turning a BigDecimal into a Postgres Numeric is
     * heavily inspired by the Intermine Implementation:
     * <p>
     * https://github.com/intermine/intermine/blob/master/intermine/objectstore/main/src/org/intermine/sql/writebatch/BatchWriterPostgresCopyImpl.java
     */
    protected void doWriteNumeric(DataOutputStream out, BigDecimal value) throws IOException {
        BigInteger unscaledValue = value.unscaledValue();

        int sign = value.signum();

        if (sign == -1) {
            unscaledValue = unscaledValue.negate();
        }

        // Number of fractional digits:
        int fractionDigits = value.scale();

        // Number of Fraction Groups:
        int fractionGroups = (fractionDigits + 3) / 4;

        List<Integer> digits = new ArrayList<>();

        // The scale needs to be a multiple of 4:
        int scaleRemainder = fractionDigits % 4;

        // Scale the first value:
        if (scaleRemainder != 0) {
            BigInteger[] result = unscaledValue.divideAndRemainder(TEN.pow(scaleRemainder));

            int digit = result[1].intValue() * (int) Math.pow(10, DECIMAL_DIGITS - scaleRemainder);

            digits.add(digit);

            unscaledValue = result[0];
        }

        while (!unscaledValue.equals(BigInteger.ZERO)) {
            BigInteger[] result = unscaledValue.divideAndRemainder(TEN_THOUSAND);
            digits.add(result[1].intValue());
            unscaledValue = result[0];
        }

        out.writeInt(8 + (2 * digits.size()));
        out.writeShort(digits.size());
        out.writeShort(digits.size() - fractionGroups - 1);
        out.writeShort(sign == 1 ? 0x0000 : 0x4000);
        out.writeShort(fractionDigits);

        // Now write each digit:
        for (int pos = digits.size() - 1; pos >= 0; pos--) {
            int valueToWrite = digits.get(pos);
            out.writeShort(valueToWrite);
        }
    }
}
