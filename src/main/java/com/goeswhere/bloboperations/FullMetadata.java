package com.goeswhere.bloboperations;

public class FullMetadata<T> {
    public final BlobMetadata<T> metadata;
    public final HashedBlob backingStore;

    public FullMetadata(BlobMetadata<T> metadata, HashedBlob backingStore) {
        this.metadata = metadata;
        this.backingStore = backingStore;
    }
}
