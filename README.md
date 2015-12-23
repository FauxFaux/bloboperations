BlobOperations
==============

BlobOperations provides a `JdbcTemplate`-like abstraction
 over `BLOB`s in PostgreSQL.

`BLOB`s are ideal for storing "large" files in the database.
 The JDBC API for them, however, is not ideal (much like the
 rest of the JDBC API.)

BlobOperations additionally adds compression and de-duplication,
 making it quicker and cheaper to store lots of data.

There are two APIs, a high-level "filesystem-like" API, and
 the underlying hashed storage mechanism.

The filesystem-like API provides names, and lets you store
 other additional metadata alongside your files:

```java
File myData = new File(some, path);

// Create a BlobStore for an existing JDBC DataSource
BlobStore store = BlobStore.forDatasource(ds);

// Fill an OutputStream with the contents of "my/key.txt"
store.store(
    "my/key.txt",
    os -> Files.copy(myData.toPath(), os));

// Read back "my/key.txt" and do some operation on it.
String heading = store.read("my/key.txt",
    (is, metadata) ->
        new BufferedReader(new InputStreamReader(is))
        .readLine()));
```

If the data written already exists in the database, then
 it will be de-duplicated on the fly.  The new data won't
  actually be stored; the old copy will be retained instead.

All synchronisation is done via the database, so the library
 is multi-machine safe.


Limitations
-----------

 * Maximum object size: Unlimited.
 * Maximum objects (after deduplication): Over 2 billion.
 * Maximum filenames and metadata: Unlimited.
 * Maximum key length: About 1GB.
 * Maximum metadata storage: About 1GB.
 * The "key" is opaque and is passed directly to Postgres;
    it can contain any characters.  There is no normalisation,
    or special treatment of `/` or `null`.


Schema
------

A sample database schema is provided in `create.pgsql`.

The classes accept table names when created, which may
 contain schemas (e.g. `foo.bar` is a valid table name).

Note that `BLOB`s do not exist *in* schemas or tables in
 PostgreSQL, so you can't clean them up with `DROP SCHEMA`,
 or `TRUNCATE TABLE`, or even `DELETE FROM TABLE`.  There
 are methods on BlobStore to delete things safely.
 `DROP DATABASE` is also sufficient.


Metadata Storage
----------------

The schema and filesystem abstraction code has space for
 carrying around a single VARCHAR of "extra" metadata,
 and machinery for serialising and deserialising it.

An example serialiser is provided in `JsonMapper.java`,
 which is not on the main classpath to avoid a `Jackson`
 dependency.

```java
  public class MyMetadata {
    public List<String> headers;
  }

  new BlobStore<>(
    HashedBlobStorage.forDatasource(ds),
    new JsonMapper().jsonStringer(
      new TypeReference<MyMetadata>() {}
    )
  ).read("key", (is, myMetadata) -> {
     myMetadata.headers...
  });
```


Deduplication
-------------

The deduplication is done based on a 128-bit truncation of
 plain `SHA-256` on the uncompressed data.  The chance of
 a false positive (claiming there's a collision when there
 is not) is around `1-in-10^18` if you have four billion
 objects stored.

 It is not currently believed that any attacker would be
 able to generate a collision in `SHA-256` except for by
 brute force, which would currently take indefeasibly long.

 It should not be hard to extend the amount of the hash
 that is used, nor to switch to a more resiliant hash
 scheme.  If you want either of these, however, you probably
 don't want deduplication and should use different code.


Testing
-------

The tests assume existence of a `test` user/database
 on the local machine, wich a password of "`test`".
 
    sudo -u postgres createuser test
    sudo -u postgres createdb -O test test
 
 See `DatabaseConnectionHelper.java`.
 