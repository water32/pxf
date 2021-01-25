package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.serializer.BaseTupleSerializer;
import org.greenplum.pxf.api.serializer.adapter.SerializerAdapter;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import static org.greenplum.pxf.plugins.hdfs.parquet.ParquetTypeConverter.bytesToTimestamp;

@Component
public class ParquetTupleSerializer extends BaseTupleSerializer<Group, MessageType> {

    @Override
    protected void writeLong(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            adapter.writeLong(out, group.getLong(columnIndex, 0));
        } else {
            raiseNotSupportedException();
        }
    }

    @Override
    protected void writeBoolean(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            adapter.writeBoolean(out, group.getBoolean(columnIndex, 0));
        } else {
            raiseNotSupportedException();
        }
    }

    @Override
    protected void writeText(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            // The string is already stored as a UTF-8 byte[]
            adapter.writeBytes(out, group.getBinary(columnIndex, 0).getBytesUnsafe());
        } else {
            // TODO: build the json. Refer to ParquetResolver#235
            raiseNotSupportedException();
        }
    }

    @Override
    protected void writeBytes(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            adapter.writeBytes(out, group.getBinary(columnIndex, 0).getBytesUnsafe());
        } else {
            raiseNotSupportedException();
        }
    }

    @Override
    protected void writeDouble(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            adapter.writeDouble(out, group.getDouble(columnIndex, 0));
        } else {
            raiseNotSupportedException();
        }
    }

    @Override
    protected void writeInteger(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            adapter.writeInteger(out, group.getInteger(columnIndex, 0));
        } else {
            raiseNotSupportedException();
        }
    }

    @Override
    protected void writeFloat(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            adapter.writeFloat(out, group.getFloat(columnIndex, 0));
        } else {
            raiseNotSupportedException();
        }
    }

    @Override
    protected void writeShort(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            adapter.writeShort(out, (short) group.getInteger(columnIndex, 0));
        } else {
            raiseNotSupportedException();
        }
    }

    @Override
    protected void writeDate(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            Type type = metadata.getType(columnIndex);
            PrimitiveType.PrimitiveTypeName primitiveType =
                    type.asPrimitiveType().getPrimitiveTypeName();

            switch (primitiveType) {
                case BINARY:
                    adapter.writeDate(out, group.getString(columnIndex, 0));
                    break;
                case INT32:
                    // The underlying integer represents the number of days
                    // since epoch
                    adapter.writeDate(out, group.getInteger(columnIndex, 0));//LocalDate.ofEpochDay(date));
                    break;
                default:
                    raiseConversionException(primitiveType, DataType.DATE, columnIndex);
                    break;
            }

        } else {
            raiseNotSupportedException();
        }
    }

    @Override
    protected void writeNumeric(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            Type type = metadata.getType(columnIndex);
            PrimitiveType.PrimitiveTypeName primitiveType =
                    type.asPrimitiveType().getPrimitiveTypeName();

            int scale;
            BigDecimal value = null;
            switch (primitiveType) {
                case INT32:
                    scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getScale();
                    value = new BigDecimal(BigInteger.valueOf(group.getInteger(columnIndex, 0)), scale);
                    break;
                case INT64:
                    scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getScale();
                    value = new BigDecimal(BigInteger.valueOf(group.getLong(columnIndex, 0)), scale);
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getScale();
                    value = new BigDecimal(new BigInteger(group.getBinary(columnIndex, 0).getBytes()), scale);
                    break;
                default:
                    raiseConversionException(primitiveType, DataType.NUMERIC, columnIndex);
                    break;
            }

            adapter.writeNumeric(out, value);
        } else {
            raiseNotSupportedException();
        }
    }

    @Override
    protected void writeTimestamp(OutputStream out, Group group, int columnIndex, MessageType metadata, SerializerAdapter adapter) throws IOException {
        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        if (repetitionCount == 0) {
            adapter.writeNull(out);
        } else if (repetitionCount == 1) {
            Type type = metadata.getType(columnIndex);
            PrimitiveType.PrimitiveTypeName primitiveType =
                    type.asPrimitiveType().getPrimitiveTypeName();

            switch (primitiveType) {
                case BINARY:
                    adapter.writeTimestamp(out, group.getString(columnIndex, 0));
                    break;
                case INT96:
                    adapter.writeTimestamp(out, bytesToTimestamp(group.getInt96(columnIndex, 0).getBytes()));
                    break;
                default:
                    raiseConversionException(primitiveType, DataType.TIMESTAMP, columnIndex);
                    break;
            }

        } else {
            raiseNotSupportedException();
        }
    }

    private void raiseConversionException(PrimitiveType.PrimitiveTypeName primitiveType, DataType greenplumType, int columnIndex) {
        throw new PxfRuntimeException(
                String.format("unable to serialize parquet %s type to Greenplum %s type", primitiveType, greenplumType),
                String.format("Make sure column %d in you Greenplum table definition matches the Parquet type.", columnIndex));
    }

    private void raiseNotSupportedException() {
        // level > 0 and type != REPEATED -- primitive type as a member of complex group -- NOT YET SUPPORTED
        throw new UnsupportedOperationException("Parquet complex type support is not yet available.");
    }
}
