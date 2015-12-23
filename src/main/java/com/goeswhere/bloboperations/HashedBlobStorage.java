package com.goeswhere.bloboperations;

import com.goeswhere.bloboperations.util.CountingOutputStream;
import com.goeswhere.bloboperations.util.InputStreamConsumer;
import com.goeswhere.bloboperations.util.NewLargeObject;
import com.goeswhere.bloboperations.util.VoidOutputStreamConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.postgresql.PGConnection;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class HashedBlobStorage {
    private static final Log logger = LogFactory.getLog(HashedBlobStorage.class);

    final JdbcOperations jdbc;
    final TransactionOperations transaction;

    public HashedBlobStorage(JdbcOperations jdbc, TransactionOperations transaction) {
        this.jdbc = jdbc;
        this.transaction = transaction;
    }

    public HashedBlob insert(VoidOutputStreamConsumer stream) {
        return transaction.execute(status -> {
            final HashedBlob stored = jdbc.execute((Connection conn) -> {
                try (final NewLargeObject largeObject = new NewLargeObject(api(conn))) {
                    return writeGeneratingMeta(stream, largeObject);
                }
            });

            // eliminate the race condition on the following "where not exists" clause by...
            // locking the whole table for write.  Not ideal, but we're expecting the transaction to
            // terminate quickly after this point, and it's better than random, hard to reproduce errors
            jdbc.execute("LOCK TABLE blob IN SHARE ROW EXCLUSIVE MODE");

            final int updated = jdbc.update("INSERT INTO blob (hash, stored_length, original_length, loid)" +
                            "SELECT ?, ?, ?, ? WHERE NOT EXISTS (" +
                            "  SELECT NULL FROM blob WHERE hash=?" +
                            ")",
                    stored.uuid, stored.storedLength, stored.originalLength, stored.oid, stored.uuid);

            if (updated != 1) {
                logger.info("we didn't actually get to do the insert; must have already existed: " + stored.uuid);
            }

            return stored;
        });
    }

    public <T> T read(UUID uuid, InputStreamConsumer<T> consumer) throws IncorrectResultSizeDataAccessException {
        return transaction.execute(status -> {
            final long oid = jdbc.queryForObject("SELECT loid FROM blob WHERE hash=?", new Object[]{uuid}, Long.class);
            return jdbc.execute((Connection conn) -> {
                final LargeObjectManager pgLOManager = api(conn);
                final LargeObject object = pgLOManager.open(oid, LargeObjectManager.READ);
                try (final GZIPInputStream inputStream = new GZIPInputStream(object.getInputStream())) {
                    return consumer.accept(inputStream);
                } catch (IOException e) {
                    throw new IllegalStateException("callee's code threw while trying to read", e);
                } finally {
                    object.close();
                }
            });
        });
    }

    public boolean exists(UUID hash) {
        return jdbc.queryForObject("SELECT EXISTS (SELECT NULL FROM blob WHERE hash=?)",
                new Object[] { hash }, Boolean.class);
    }

    public void delete(long storageOid) {
        transaction.execute(status -> {
            // this return value of "1" doesn't seem to be documented, but it seems worth checking
            if (1 != jdbc.queryForObject("SELECT lo_unlink(?)", new Object[]{storageOid}, Integer.class)) {
                throw new IllegalStateException("couldn't delete object " + storageOid);
            }

            final int update = jdbc.update("DELETE FROM blob WHERE loid=?", storageOid);
            if (1 != update) {
                throw new IncorrectResultSizeDataAccessException(1, update);
            }

            return null;
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
                        uuid(digest.digest()),
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

    private static UUID uuid(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }
}
