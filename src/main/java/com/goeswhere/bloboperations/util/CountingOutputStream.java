package com.goeswhere.bloboperations.util;

import java.io.IOException;
import java.io.OutputStream;

public class CountingOutputStream extends OutputStream {

    private final OutputStream out;
    private long count;

    public CountingOutputStream(OutputStream out) {
        this.out = out;
    }

    public long getCount() {
        return count;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        count += len;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        count++;
    }

    @Override
    public void flush() throws IOException {
        out.flush();
        super.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
        super.close();
    }
}
