package com.goeswhere.bloboperations;

import com.fasterxml.jackson.core.type.TypeReference;
import com.goeswhere.bloboperations.helpers.JsonMapper;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;

public class BlobStoreTest extends DatabaseConnectionHelper {

    public static class Foo {
        public Foo(int bar) {
            this.bar = bar;
        }

        public Foo() {
            // for Jackson
        }

        public int bar;
    }

    BlobStore<Foo> store = new BlobStore<>(
            new HashedBlobStorage(jdbc, transactions, "blopstest.blob"),
            new JsonMapper().jsonStringer(
            new TypeReference<Foo>() {
            }),
            "blopstest.metadata");

    @Test
    public void testEmpty() {
        store.store("foo", os -> new Foo(7));
        store.read("foo", (is, foo) -> {
            assertEquals(7, foo.extra.bar);
            assertEquals(-1, is.read());
            return null;
        });
    }

    @Test
    public void conflict() {
        store.store("bar", os -> null);
        try {
            store.store("bar", os -> {
                os.write(5);
                return null;
            });
            fail("expected exception");
        } catch (IllegalStateException alreadyExists) {

        }
    }

    @Test
    public void delete() {
        store.store("baz", os -> null);
        store.delete("baz");
    }

    @Test
    public void apparentSizes() {
        assertEquals(0, store.directoryApparentSize("a/"));
        writeHelloWorld("a/foo");
        final long justOne = store.directoryApparentSize("a/");
        assertNotSame(0, justOne);
        writeHelloWorld("a/bar");
        assertNotSame(justOne, store.directoryApparentSize("a/"));
    }

    private void writeHelloWorld(String key) {
        store.store(key, os -> {
            os.write("hello world".getBytes(StandardCharsets.UTF_8));
            return null;
        });
    }

    @Test(expected = NoSuchElementException.class)
    public void deleteMissing() {
        store.delete("quux");
    }

    @Test
    public void collect() {
        final UUID hashOfQ = UUID.fromString("8e35c2cd-3bf6-641b-db0e-2050b76932cb");

        assertFalse(store.storage.exists(hashOfQ));

        store.store("coll", os -> {
            os.write('q');
            return null;
        });

        assertTrue(store.storage.exists(hashOfQ));

        store.delete("coll");

        assertTrue(store.storage.exists(hashOfQ));

        store.collectGarbage();

        assertFalse(store.storage.exists(hashOfQ));
    }
}
