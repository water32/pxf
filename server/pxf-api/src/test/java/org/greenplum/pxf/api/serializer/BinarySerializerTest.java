package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.QuerySession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class BinarySerializerTest {

    static Stream<DataType> dataTypeValues() {
        return Arrays.stream(DataType.values());
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    @Test
    void testWriteEmptyStream() throws IOException {
        try (BinarySerializer serializer = new BinarySerializer()) {
            serializer.open(out);
        }
        int offset = 0;
        byte[] result = out.toByteArray();
        offset = verifyHeader(result, offset);
        verifyClose(result, offset);
    }

    @Test
    void testWriteStartRowEndRow() throws IOException {
        try (BinarySerializer serializer = new BinarySerializer()) {
            serializer.open(out);
            serializer.startRow(0);
            serializer.endRow();
        }
        int offset = 0;
        byte[] result = out.toByteArray();
        offset = verifyHeader(result, offset);
        offset = verifyStartRow(result, offset, 0);
        verifyClose(result, offset);
    }

    @MethodSource("dataTypeValues")
    @ParameterizedTest
    @SuppressWarnings("unchecked")
    void testWriteNull(DataType dataType) throws IOException {
        int numberOfFields = 1;
        QuerySession<String> session = mock(QuerySession.class);
        try (BinarySerializer serializer = new BinarySerializer()) {
            serializer.open(out);
            serializer.startRow(numberOfFields);
            serializer.startField();
            serializer.writeField(dataType, (s, o, c) -> null, session, "5L", 0);
            serializer.endField();
            serializer.endRow();
        }
        int offset = 0;
        byte[] result = out.toByteArray();
        offset = verifyHeader(result, offset);
        offset = verifyStartRow(result, offset, numberOfFields);
        offset = verifyNull(result, offset);
        verifyClose(result, offset);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testWrite_BIGINT() throws IOException {
        int numberOfFields = 1;
        QuerySession<String> session = mock(QuerySession.class);
        try (BinarySerializer serializer = new BinarySerializer()) {
            serializer.open(out);
            serializer.startRow(numberOfFields);
            serializer.startField();
            serializer.writeField(DataType.BIGINT, (s, o, c) -> 5L, session, "5L", 0);
            serializer.endField();
            serializer.endRow();
        }
        int offset = 0;
        byte[] result = out.toByteArray();
        offset = verifyHeader(result, offset);
        offset = verifyStartRow(result, offset, numberOfFields);

        // verify that we write the length of bigint (8 bytes)
        assertThat(result[offset++]).isEqualTo((byte) (8 >>> 24));
        assertThat(result[offset++]).isEqualTo((byte) (8 >>> 16));
        assertThat(result[offset++]).isEqualTo((byte) (8 >>> 8));
        assertThat(result[offset++]).isEqualTo((byte) 8);

        // verify that we write the actual value of the long
        assertThat(result[offset++]).isEqualTo((byte) (5L >>> 56));
        assertThat(result[offset++]).isEqualTo((byte) (5L >>> 48));
        assertThat(result[offset++]).isEqualTo((byte) (5L >>> 40));
        assertThat(result[offset++]).isEqualTo((byte) (5L >>> 32));
        assertThat(result[offset++]).isEqualTo((byte) (5L >>> 24));
        assertThat(result[offset++]).isEqualTo((byte) (5L >>> 16));
        assertThat(result[offset++]).isEqualTo((byte) (5L >>> 8));
        assertThat(result[offset++]).isEqualTo((byte) (5L));

        verifyClose(result, offset);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCastExceptionOnWrite_BIGINT() throws IOException {
        int numberOfFields = 1;
        QuerySession<String> session = mock(QuerySession.class);
        try (BinarySerializer serializer = new BinarySerializer()) {
            serializer.open(out);
            serializer.startRow(numberOfFields);
            serializer.startField();
            // Return a string for a BIGINT type
            assertThatThrownBy(() -> serializer.writeField(DataType.BIGINT, (s, o, c) -> "foobar", session, "5L", 0))
                    .isInstanceOf(ClassCastException.class);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testWrite_BOOLEAN() throws IOException {
        int numberOfFields = 1;
        QuerySession<String> session = mock(QuerySession.class);
        try (BinarySerializer serializer = new BinarySerializer()) {
            serializer.open(out);
            serializer.startRow(numberOfFields);
            serializer.startField();
            serializer.writeField(DataType.BOOLEAN, (s, o, c) -> true, session, "true", 0);
            serializer.endField();
            serializer.endRow();
            serializer.startRow(numberOfFields);
            serializer.startField();
            serializer.writeField(DataType.BOOLEAN, (s, o, c) -> false, session, "false", 0);
            serializer.endField();
            serializer.endRow();
        }
        int offset = 0;
        byte[] result = out.toByteArray();
        offset = verifyHeader(result, offset);
        offset = verifyStartRow(result, offset, numberOfFields);

        // verify that we write the length of bigint (8 bytes)
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 1);

        // verify that we write the actual value of the long
        assertThat(result[offset++]).isEqualTo((byte) 1);

        offset = verifyStartRow(result, offset, numberOfFields);

        // verify that we write the length of bigint (8 bytes)
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 1);

        // verify that we write the actual value of the long
        assertThat(result[offset++]).isEqualTo((byte) 0);

        verifyClose(result, offset);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testWrite_BPCHAR() throws IOException {
        int numberOfFields = 1;
        QuerySession<String> session = mock(QuerySession.class);
        try (BinarySerializer serializer = new BinarySerializer()) {
            serializer.open(out);
            serializer.startRow(numberOfFields);
            serializer.startField();
            serializer.writeField(DataType.BPCHAR, (s, o, c) -> "foo", session, "foo", 0);
            serializer.endField();
            serializer.endRow();
        }
        int offset = 0;
        byte[] result = out.toByteArray();
        offset = verifyHeader(result, offset);
        offset = verifyStartRow(result, offset, numberOfFields);

        // verify that we write the length of the bytes of the string (variable)
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 3);

        // verify that we write the actual value of the string
        assertThat(result[offset++]).isEqualTo((byte) 'f');
        assertThat(result[offset++]).isEqualTo((byte) 'o');
        assertThat(result[offset++]).isEqualTo((byte) 'o');

        verifyClose(result, offset);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testWrite_TEXT() throws IOException {
        int numberOfFields = 1;
        QuerySession<String> session = mock(QuerySession.class);
        try (BinarySerializer serializer = new BinarySerializer()) {
            serializer.open(out);
            serializer.startRow(numberOfFields);
            serializer.startField();
            serializer.writeField(DataType.TEXT, (s, o, c) -> "bar", session, "bar", 0);
            serializer.endField();
            serializer.endRow();
        }
        int offset = 0;
        byte[] result = out.toByteArray();
        offset = verifyHeader(result, offset);
        offset = verifyStartRow(result, offset, numberOfFields);

        // verify that we write the length of the bytes of the string (variable)
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 3);

        // verify that we write the actual value of the string
        assertThat(result[offset++]).isEqualTo((byte) 'b');
        assertThat(result[offset++]).isEqualTo((byte) 'a');
        assertThat(result[offset++]).isEqualTo((byte) 'r');

        verifyClose(result, offset);
    }

    int verifyHeader(byte[] result, int offset) {
        // 11 bytes required header
        assertThat(result[offset++]).isEqualTo((byte) 'P');
        assertThat(result[offset++]).isEqualTo((byte) 'G');
        assertThat(result[offset++]).isEqualTo((byte) 'C');
        assertThat(result[offset++]).isEqualTo((byte) 'O');
        assertThat(result[offset++]).isEqualTo((byte) 'P');
        assertThat(result[offset++]).isEqualTo((byte) 'Y');
        assertThat(result[offset++]).isEqualTo((byte) '\n');
        assertThat(result[offset++]).isEqualTo((byte) '\377');
        assertThat(result[offset++]).isEqualTo((byte) '\r');
        assertThat(result[offset++]).isEqualTo((byte) '\n');
        assertThat(result[offset++]).isEqualTo((byte) '\0');

        // 32 bit integer indicating no OID
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        // 32 bit header extension area length
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        assertThat(result[offset++]).isEqualTo((byte) 0);
        return offset;
    }

    int verifyStartRow(byte[] result, int offset, int numberOfFields) {
        // Each tuple begins with a 16-bit integer count of the number of fields in the tuple
        assertThat(result[offset++]).isEqualTo((byte) (numberOfFields >>> 8));
        assertThat(result[offset++]).isEqualTo((byte) numberOfFields);
        return offset;
    }

    private int verifyNull(byte[] result, int offset) {
        // As a special case, -1 indicates a NULL field value.
        // No value bytes follow in the NULL case.
        assertThat(result[offset++]).isEqualTo((byte) (-1 >>> 24));
        assertThat(result[offset++]).isEqualTo((byte) (-1 >>> 16));
        assertThat(result[offset++]).isEqualTo((byte) (-1 >>> 8));
        assertThat(result[offset++]).isEqualTo((byte) -1);
        return offset;
    }

    void verifyClose(byte[] result, int offset) {
        // The file trailer consists of a 16-bit integer word containing -1.
        assertThat(result[offset++]).isEqualTo((byte) 255);
        assertThat(result[offset]).isEqualTo((byte) 255);
    }
}