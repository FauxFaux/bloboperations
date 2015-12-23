package com.goeswhere.bloboperations.util;

import java.util.function.Function;

public class Stringer<T> {
    public final Function<String, T> fromString;
    public final Function<T, String> toString;

    public Stringer(Function<String, T> fromString, Function<T, String> toString) {
        this.fromString = fromString;
        this.toString = toString;
    }

    public static <T> Stringer<T> alwaysNull() {
        return new Stringer<>(x -> null, x -> null);
    }
}
