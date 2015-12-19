package com.goeswhere.bloboperations;

public class HashedBlob {
    public final Hash uuid;
    public final long storedLength;
    public final long originalLength;

    public HashedBlob(Hash uuid, long storedLength, long originalLength) {
        this.uuid = uuid;
        this.storedLength = storedLength;
        this.originalLength = originalLength;
    }
}
