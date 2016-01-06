package com.goeswhere.bloboperations.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class BlockCloseOutputStream extends FilterOutputStream {
    public BlockCloseOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void close() throws IOException {
        // no thanks!
    }
}
