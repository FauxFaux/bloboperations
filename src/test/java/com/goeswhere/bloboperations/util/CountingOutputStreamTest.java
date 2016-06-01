package com.goeswhere.bloboperations.util;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

public class CountingOutputStreamTest {

    @Test
    public void test() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final CountingOutputStream cos = new CountingOutputStream(baos);
        assertEquals(0, cos.getCount());

        cos.write(4);
        assertEquals(1, cos.getCount());
        assertEquals(1, baos.size());

        cos.write(new byte[] { 1, 2 });
        assertEquals(3, cos.getCount());
        assertEquals(3, baos.size());

        cos.write(new byte[] { 3, 4, 5, 6, 7, 8}, 2, 3);
        assertEquals(6, cos.getCount());
        assertEquals(6, baos.size());
    }
}