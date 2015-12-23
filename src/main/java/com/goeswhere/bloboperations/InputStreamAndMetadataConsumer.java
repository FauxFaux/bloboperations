package com.goeswhere.bloboperations;

import java.io.IOException;
import java.io.InputStream;

@FunctionalInterface
public interface InputStreamAndMetadataConsumer<T, EX> {
    T accept(InputStream inputStream, BlobMetadata<EX> metadata) throws IOException;
}