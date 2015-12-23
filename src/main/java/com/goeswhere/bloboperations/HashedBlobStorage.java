package com.goeswhere.bloboperations;

import com.goeswhere.bloboperations.util.*;
import org.postgresql.PGConnection;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;

import java.io.IOException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class HashedBlobStorage {
    private final JdbcOperations jdbc;
    private final TransactionOperations transaction;

    public HashedBlobStorage(JdbcOperations jdbc, TransactionOperations transaction) {
        this.jdbc = jdbc;
        this.transaction = transaction;
    }

    HashedBlob insert(VoidOutputStreamConsumer stream) {
        return transaction.execute(status -> {
            final HashedBlob stored = jdbc.execute((Connection conn) -> {
                try (final NewLargeObject largeObject = new NewLargeObject(api(conn))) {
                    try {
                        return writeGeneratingMeta(stream, largeObject);
                    } catch (Throwable t) {
                        t.printStackTrace();
                        throw t;
                    }
                }
            });

            try {
                jdbc.update("INSERT INTO blob (hash, stored_length, original_length, loid) VALUES (?, ?, ?, ?)",
                        stored.uuid, stored.storedLength, stored.originalLength, stored.oid);
            } catch (DuplicateKeyException ignored) {
                // Our transaction has errored, so is going to rollback.
                // Not going to bother to refresh the returned object; the values may be wrong
                // (the loid certainly is), but they're not really part of our exposed api.
            }

            return stored;
        });
    }

    <T> T read(UUID uuid, InputStreamConsumer<T> consumer) throws IncorrectResultSizeDataAccessException {
        return transaction.execute(status -> {
            final long oid = jdbc.queryForObject("SELECT loid FROM blob WHERE hash=?", new Object[]{uuid}, Long.class);
            return jdbc.execute((Connection conn) -> {
                final LargeObjectManager pgLOManager = api(conn);
                final LargeObject object = pgLOManager.open(oid, LargeObjectManager.READ);
                try (final GZIPInputStream inputStream = new GZIPInputStream(object.getInputStream())) {
                    return consumer.accept(inputStream);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                } finally {
                    object.close();
                }
            });
        });
    }

    private static HashedBlob writeGeneratingMeta(VoidOutputStreamConsumer stream, NewLargeObject largeObject) throws SQLException {
        return largeObject.write(dbOs -> {
            final MessageDigest digest = digest();
            // nested output streams are applied in reading order; we take the callers values,
            // we count them, then we digest them, then we gzip them...
            // then the countingToDb counts them, then they go to the db.

            // dbOs doesn't like being closed, so we'll just flush it and close it outside
            final CountingOutputStream countingToDb = new CountingOutputStream(dbOs);
            try (final CountingOutputStream countingFromCaller = new CountingOutputStream(
                    new DigestOutputStream(new GZIPOutputStream(countingToDb), digest))) {

                stream.accept(countingFromCaller);
                countingToDb.flush();

                return new HashedBlob(
                        Hash.uuid(digest.digest()),
                        countingToDb.getCount(),
                        countingFromCaller.getCount(),
                        largeObject.getOid());

            } catch (IOException e) {
                throw new IllegalStateException("couldn't construct blob", e);
            }
        });
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static LargeObjectManager api(Connection conn) throws SQLException {
        return conn.unwrap(PGConnection.class).getLargeObjectAPI();
    }
}
