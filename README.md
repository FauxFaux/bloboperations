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

Schema
------

A sample database schema is provided in `create.pgsql`.

The classes accept table names when created, which may
 contain schemas (e.g. `foo.bar` is a valid table name).

Note that `BLOB`s do not exist *in* schemas or tables in
 PostgreSQL, so you can't clean them up with `DROP SCHEMA`,
 or `TRUNCATE TABLE`, or even `DELETE FROM TABLE`.  There
 are methods on BlobStore to delete things safely.

Testing
-------

The tests assume existence of a `test` user/database
 on the local machine, wich a password of "`test`".
 
    sudo -u postgres createuser test
    sudo -u postgres createdb -O test test
 
 See `DatabaseConnectionHelper.java`.
 