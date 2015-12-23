package com.goeswhere.bloboperations;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;

public class HashedBlobStorageTest {

    private static final int EOF = -1;

    static DataSource ds;
    static JdbcTemplate jdbc;

    @BeforeClass
    public static void connect() {
        ds = new DriverManagerDataSource("jdbc:postgresql:test");
        jdbc = new JdbcTemplate(ds);
        jdbc.execute("TRUNCATE TABLE blob");
    }

    final HashedBlobStorage storage = new HashedBlobStorage(
            jdbc,
            new TransactionTemplate(new DataSourceTransactionManager(ds)));

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