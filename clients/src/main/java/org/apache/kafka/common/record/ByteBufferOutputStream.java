package org.apache.kafka.common.record;

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A byte buffer backed output outputStream
 */
public class ByteBufferOutputStream extends OutputStream {

    private static final float REALLOCATION_FACTOR = 1.1f;

    private ByteBuffer buffer;

    public ByteBufferOutputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void write(int b) {
        if (buffer.remaining() < 1)
            expandBuffer(buffer.capacity() + 1);
        buffer.put((byte) b);
    }

    public void write(byte[] bytes, int off, int len) {
        if (buffer.remaining() < len)
            expandBuffer(buffer.capacity() + len);
        buffer.put(bytes, off, len);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    private void expandBuffer(int size) {
        int expandSize = Math.max((int) (buffer.capacity() * REALLOCATION_FACTOR), size);
        ByteBuffer temp = ByteBuffer.allocate(expandSize);
        temp.put(buffer.array(), buffer.arrayOffset(), buffer.position());
        buffer = temp;
    }
}
