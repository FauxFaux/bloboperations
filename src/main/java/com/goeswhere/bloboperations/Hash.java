package com.goeswhere.bloboperations;

import java.nio.ByteBuffer;
import java.util.UUID;

public class Hash {
    public static UUID uuid(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }
}
