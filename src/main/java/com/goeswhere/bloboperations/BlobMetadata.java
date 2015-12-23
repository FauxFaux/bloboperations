package com.goeswhere.bloboperations;

import java.time.ZonedDateTime;

public class BlobMetadata<T> {
    public final String key;
    public final T extra;
    public final ZonedDateTime created;
    public final long storedLength;
    public final long originalLength;
    final long storageOid;

    public BlobMetadata(String key,
                        T extra,
                        ZonedDateTime created,
                        long storedLength,
                        long originalLength,
                        long storageOid) {
        this.key = key;
        this.originalLength = originalLength;
        this.extra = extra;
        this.created = created;
        this.storedLength = storedLength;
        this.storageOid = storageOid;
    }
}
