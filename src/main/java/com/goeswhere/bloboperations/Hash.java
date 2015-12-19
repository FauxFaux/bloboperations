package com.goeswhere.bloboperations;

import java.util.Arrays;

public class Hash {
    private final byte[] bytes;

    Hash(byte[] bytes) {
        assert bytes.length == 16;
        this.bytes = Arrays.copyOf(bytes, 16);
    }
}
