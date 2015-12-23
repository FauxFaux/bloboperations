package com.goeswhere.bloboperations;

import com.fasterxml.jackson.core.type.TypeReference;
import com.goeswhere.bloboperations.util.JsonMapper;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

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
            new HashedBlobStorage(jdbc, transactions),
            new JsonMapper(),
            new TypeReference<Foo>() {
            });

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
}
