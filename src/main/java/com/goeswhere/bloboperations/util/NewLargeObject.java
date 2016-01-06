package com.goeswhere.bloboperations.util;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

public class NewLargeObject implements AutoCloseable {

    private final long newOid;
    private final LargeObject objectInDb;

    public NewLargeObject(LargeObjectManager pgLOManager) throws SQLException {
        newOid = pgLOManager.createLO(LargeObjectManager.WRITE);
        // oid creation will be rolled-back at the end of the transaction if anything goes wrong

        objectInDb = pgLOManager.open(newOid, LargeObjectManager.WRITE);
    }

    public <T> T write(OutputStreamConsumer<T> consumer) throws SQLException {
        try (final OutputStream outputStream = objectInDb.getOutputStream()) {
            return consumer.accept(outputStream);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public long getOid() {
        return newOid;
    }

    @Override
    public void close() throws SQLException {
        // it is safe to close this multiple times
        objectInDb.close();
    }
}
