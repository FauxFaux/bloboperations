package com.goeswhere.bloboperations.util;

import java.io.IOException;
import java.io.OutputStream;

@FunctionalInterface
public interface OutputStreamConsumer<T> {
    T accept(OutputStream outputStream) throws IOException;
}
