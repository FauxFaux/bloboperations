package com.goeswhere.bloboperations;

import com.goeswhere.bloboperations.util.CountingOutputStream;
import org.postgresql.PGConnection;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.zip.GZIPOutputStream;

public class HashedBlobStorage {
    JdbcOperations jdbc;
    TransactionOperations transaction;

    @FunctionalInterface
    public interface OutputStreamConsumer {
        void accept(OutputStream outputStream) throws IOException;
    }

    @FunctionalInterface
    public interface InputStreamConsumer<T, EX> {
        T accept(InputStream inputStream, BlobMetadata<EX> metadata) throws IOException;
    }

    HashedBlob insert(OutputStreamConsumer stream) {
        return transaction.execute(status -> jdbc.execute((Connection conn) -> {
                    final MessageDigest digest = digest();

                    final LargeObjectManager pgLOManager = api(conn);
                    final long newOid = pgLOManager.createLO(LargeObjectManager.WRITE);
                    LargeObject objectInDb = null;

                    boolean success = false;
                    try {
                        objectInDb = pgLOManager.open(newOid, LargeObjectManager.WRITE);
                        // nested output streams are applied in reading order; we take the callers values,
                        // we count them, then we digest them, then we gzip them...
                        // then the countingToDb counts them, then they go to the db.
                        try (final CountingOutputStream countingToDb = new CountingOutputStream(objectInDb.getOutputStream());
                             final CountingOutputStream countingFromCaller = new CountingOutputStream(
                                     new DigestOutputStream(new GZIPOutputStream(countingToDb), digest))) {

                            stream.accept(countingFromCaller);

                            // not convinced this is necessary, but seems like it may protect against some problems
                            countingFromCaller.flush();

                            success = true;
                            return new HashedBlob(new Hash(digest.digest()), countingToDb.getCount(), countingFromCaller.getCount());
                        } catch (IOException e) {
                            throw new IllegalStateException("couldn't construct blob", e);
                        }
                    } finally {
                        // if we opened an object, it must be closed;
                        // on success, this will flush; on both, it will free any driver resources
                        if (null != objectInDb) {
                            objectInDb.close();
                        }

                        // if we didn't complete our write, then ensure the oid is cleaned up
                        if (!success) {
                            pgLOManager.delete(newOid);
                        }
                    }
                }
        ));
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private LargeObjectManager api(Connection conn) throws SQLException {
        return conn.unwrap(PGConnection.class).getLargeObjectAPI();
    }
}
