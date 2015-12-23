package com.goeswhere.bloboperations.util;

import java.io.IOException;
import java.io.InputStream;

@FunctionalInterface
public interface InputStreamConsumer<T> {
    T accept(InputStream outputStream) throws IOException;
}
