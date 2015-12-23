package com.goeswhere.bloboperations;

import java.time.ZonedDateTime;
import java.util.UUID;

public class BlobMetadata<T> {
    public final String key;
    public final ZonedDateTime created;
    public final UUID hash;
    public final T extra;

    public BlobMetadata(String key, ZonedDateTime created, UUID hash, T extra) {
        this.key = key;
        this.created = created;
        this.hash = hash;
        this.extra = extra;
    }
}
