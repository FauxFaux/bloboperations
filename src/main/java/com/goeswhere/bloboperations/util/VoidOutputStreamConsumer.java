package com.goeswhere.bloboperations.util;

import java.io.IOException;
import java.io.OutputStream;

@FunctionalInterface
public interface VoidOutputStreamConsumer {
    void accept(OutputStream outputStream) throws IOException;
}
