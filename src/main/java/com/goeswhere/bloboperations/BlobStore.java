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

    final HashedBlobStorage storage;
    private final Stringer<EX> serialiseExtra;

    public BlobStore(HashedBlobStorage storage, Stringer<EX> serialiseExtra) {
        this.storage = storage;
        this.serialiseExtra = serialiseExtra;
    }

    public static <T> BlobStore<T> forDatasource(DataSource ds) {
        return new BlobStore<>(HashedBlobStorage.forDatasource(ds), Stringer.alwaysNull());
    }

    public EX store(String key, OutputStreamConsumer<EX> data) {
        // outside of the transaction, ensure that the row exists, so we can lock it.
        try {
            storage.jdbc.update("INSERT INTO metadata (key, created) VALUES (?, now())", key);
        } catch (DuplicateKeyException ignored) {
            log.info("there was a metadata key collision, but it might not be fatal; continuing.  key=" + key);
        }

        return storage.transaction.execute(status -> {
            final UUID existing = storage.jdbc.queryForObject(
                    "SELECT hash FROM metadata WHERE key=? FOR UPDATE",
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
                    "UPDATE metadata SET hash=?, extra=? WHERE key=?",
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
                "SELECT created, hash, extra FROM metadata WHERE key=? FOR SHARE",
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
        if (1 != storage.jdbc.update("DELETE FROM metadata WHERE key=?", key)) {
            throw new NoSuchElementException("couldn't delete key " + key + " as it didn't exist");
        }
    }

    public void collectGarbage() {
        storage.transaction.execute(status -> {
            storage.jdbc.query("SELECT loid FROM blob WHERE NOT EXISTS (" +
                    "  SELECT NULL FROM metadata WHERE blob.hash=metadata.hash" +
                    ") FOR UPDATE", (rs, underscore) -> rs.getLong("loid"))
                    .forEach(storage::delete);
            return null;
        });
    }
}