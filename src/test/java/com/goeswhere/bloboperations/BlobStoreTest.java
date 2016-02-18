package com.goeswhere.bloboperations;

import com.fasterxml.jackson.core.type.TypeReference;
import com.goeswhere.bloboperations.helpers.JsonMapper;
import org.junit.Test;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

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
            new HashedBlobStorage(jdbc, transactions, "blopstest.blob", HashedBlobStorage.GZIP_STORAGE_FILTER),
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

    @Test(expected = IncorrectResultSizeDataAccessException.class)
    public void updateMissingMetadata() {
        store.updateUserMetadata("johnson", new Foo());
    }

    @Test
    public void updateMetadata() {
        writeHelloWorld("meaty");
        assertNull(store.metadata("meaty").extra);
        store.updateUserMetadata("meaty", new Foo(12));
        assertEquals(12, store.metadata("meaty").extra.bar);
    }

    @Test(expected = NoSuchElementException.class)
    public void deleteMissing() {
        store.delete("quux");
    }

    @Test
    public void blobExists() {
        assertFalse(store.exists("existy"));
        writeHelloWorld("existy");
        assertTrue(store.exists("existy"));
        store.delete("existy");
        assertFalse(store.exists("existy"));
    }

    @Test
    public void deletePrefix() {
        writeHelloWorld("pre/a");
        writeHelloWorld("pre/b");
        writeHelloWorld("preb");
        store.deletePrefix("pre/");
        assertFalse(store.exists("pre/a"));
        assertFalse(store.exists("pre/b"));
        assertTrue(store.exists("preb"));
    }

    @Test
    public void fullMetadata() {
        writeHelloWorld("full-data");
        final FullMetadata<Foo> full = store.fullMetadata("full-data");
        assertEquals("full-data", full.metadata.key);
        assertEquals("hello world".length(), full.backingStore.originalLength);
    }

    @Test
    public void fullMetadataList() {
        writeHelloWorld("full/a");
        writeHelloWorld("full/b");
        store.store("full/c", os -> new Foo(5));

        final List<FullMetadata<Foo>> datas = store.listFullMetadataByPrefix("full/");
        assertEquals(3, datas.size());

        assertEquals(new HashSet<>(Arrays.asList("full/a", "full/b", "full/c")),
                datas.stream().map(meta -> meta.metadata.key).collect(Collectors.toSet()));

        assertEquals(new HashSet<>(Arrays.asList(0L, (long) "hello world".length())),
                datas.stream().map(meta -> meta.backingStore.originalLength).collect(Collectors.toSet()));
    }

    @Test
    public void userCloses() {
        store.store("close", os -> {
            try (OutputStream buff = new BufferedOutputStream(os)) {
                buff.write("you think you're".getBytes(StandardCharsets.UTF_8));
            }

            return null;
        });
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
