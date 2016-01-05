package com.goeswhere.bloboperations;

import com.goeswhere.bloboperations.util.OutputStreamConsumer;
import com.goeswhere.bloboperations.util.Stringer;
import com.goeswhere.bloboperations.util.VoidOutputStreamConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.NoSuchElementException;
import java.util.UUID;

public class BlobStore<EX> {
    private static final Log log = LogFactory.getLog(BlobStore.class);

    public static final String DEFAULT_TABLE_NAME = "metadata";

    final HashedBlobStorage storage;
    private final Stringer<EX> serialiseExtra;

    private final String metadataTableName;

    public BlobStore(
            HashedBlobStorage storage,
            Stringer<EX> serialiseExtra) {
        this(storage, serialiseExtra, DEFAULT_TABLE_NAME);
    }

    public BlobStore(
            HashedBlobStorage storage,
            Stringer<EX> serialiseExtra,
            String metadataTableName) {
        this.storage = storage;
        this.serialiseExtra = serialiseExtra;
        this.metadataTableName = metadataTableName;
    }

    public static <T> BlobStore<T> forDatasource(DataSource ds) {
        return new BlobStore<>(HashedBlobStorage.forDatasource(ds), Stringer.alwaysNull());
    }

    public EX store(String key, OutputStreamConsumer<EX> data) {
        // outside of the transaction, ensure that the row exists, so we can lock it.
        try {
            storage.jdbc.update("INSERT INTO " + metadataTableName + " (key, created) VALUES (?, now())", key);
        } catch (DuplicateKeyException ignored) {
            log.info("there was a metadata key collision, but it might not be fatal; continuing.  key=" + key);
        }

        return storage.transaction.execute(status -> {
            final UUID existing = storage.jdbc.queryForObject(
                    "SELECT hash FROM " + metadataTableName + " WHERE key=? FOR UPDATE",
                    new Object[]{key}, UUID.class);

            if (null != existing) {
                throw new IllegalStateException(key + " already exists");
            }

            class Capture implements VoidOutputStreamConsumer {
                EX extra;

                @Override
                public void accept(OutputStream outputStream) throws IOException {
                    extra = data.accept(outputStream);
                }
            }

            final Capture cap = new Capture();
            final HashedBlob hashed = storage.insert(cap);

            final int updated = storage.jdbc.update(
                    "UPDATE " + metadataTableName + " SET hash=?, extra=? WHERE key=?",
                    hashed.uuid, serialiseExtra.toString.apply(cap.extra), key);

            if (1 != updated) {
                throw new IncorrectResultSizeDataAccessException("couldn't set metadata", 1, updated);
            }

            return cap.extra;
        });
    }

    public BlobMetadata<EX> metadata(String key) {
        // FOR SHARE prevents the row from being deleted, which will prevent
        // (at an application level) the blob from being deleted before we read it
        return storage.jdbc.queryForObject(
                "SELECT created, hash, extra FROM " + metadataTableName + " WHERE key=? FOR SHARE",
                new Object[]{key}, (rs, underscore) -> {
                    return new BlobMetadata<>(
                            key,
                            ZonedDateTime.ofInstant(rs.getTimestamp("created").toInstant(), ZoneOffset.UTC),
                            (UUID) rs.getObject("hash"),
                            serialiseExtra.fromString.apply(rs.getString("extra")));
                });
    }

    public <T> T read(String key, InputStreamAndMetadataConsumer<T, EX> consumer) {
        return storage.transaction.execute(status -> {
            final BlobMetadata<EX> metadata = metadata(key);
            return storage.read(metadata.hash, is -> consumer.accept(is, metadata));
        });
    }

    public void delete(String key) {
        if (1 != storage.jdbc.update("DELETE FROM " + metadataTableName + " WHERE key=?", key)) {
            throw new NoSuchElementException("couldn't delete key " + key + " as it didn't exist");
        }
    }

    public long directoryApparentSize(String directory) {
        final Long sum = storage.jdbc.queryForObject("SELECT SUM(stored_length)" +
                " FROM " + storage.blobTableName +
                " INNER JOIN " + metadataTableName + "" +
                " USING (hash)" +
                " WHERE key LIKE ?", new Object[]{directory + "%"}, Long.class);
        return null != sum ? sum : 0;
    }

    public void collectGarbage() {
        storage.transaction.execute(status -> {
            storage.jdbc.query("SELECT loid FROM " + storage.blobTableName + " WHERE NOT EXISTS (" +
                    "  SELECT NULL FROM " + metadataTableName + "" +
                    "    WHERE " + storage.blobTableName + ".hash=" + metadataTableName + ".hash" +
                    ") FOR UPDATE", (rs, underscore) -> rs.getLong("loid"))
                    .forEach(storage::delete);
            return null;
        });
    }
}
