package com.goeswhere.bloboperations;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;

public class HashedBlobStorageTest extends DatabaseConnectionHelper {

    private static final int EOF = -1;

    final HashedBlobStorage storage = new HashedBlobStorage(
            jdbc,
            transactions,
            "blopstest.blob", HashedBlobStorage.GZIP_STORAGE_FILTER);

    @Test
    public void testInsertEmpty() {
        final HashedBlob blob = storage.insert(os -> {
            // nothing to see here!
        });

        storage.read(blob.uuid, is -> {
            assertEquals(EOF, is.read());
            return null;
        });
    }

    @Test
    public void testInsertString() {
        final HashedBlob blob = storage.insert(os -> os.write("hello world".getBytes(StandardCharsets.UTF_8)));

        assertEquals("hello world", storage.read(blob.uuid, is -> {
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                try {
                    return reader.readLine();
                } finally {
                    assertNull(reader.readLine());
                }
            }
        }));
    }

    @Test
    public void writeByteArray() {
        final byte[][] bytes = {
                new byte[]{0},
                new byte[]{1, 2, 3},
                new byte[]{4},
                new byte[0],
                new byte[]{5, 6, 7, 8}
        };

        final HashedBlob stored = storage.insert(os -> {
            for (byte[] blob : bytes) {
                os.write(blob);
            }
        });

        storage.read(stored.uuid, is -> {
            for (byte[] expectedPart : bytes) {
                final byte[] actualPart = new byte[expectedPart.length];
                assertEquals(actualPart.length, is.read(actualPart));
                assertArrayEquals(expectedPart, actualPart);
            }
            assertEquals(EOF, is.read());

            return null;
        });
    }
}