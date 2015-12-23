package com.goeswhere.bloboperations;

import java.util.UUID;

public class HashedBlob {
    public final UUID uuid;
    public final long storedLength;
    public final long originalLength;
    final long oid;

    public HashedBlob(UUID uuid, long storedLength, long originalLength, long oid) {
        this.uuid = uuid;
        this.storedLength = storedLength;
        this.originalLength = originalLength;
        this.oid = oid;
    }
}
