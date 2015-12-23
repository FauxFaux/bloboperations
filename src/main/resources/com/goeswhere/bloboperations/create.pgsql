CREATE TABLE blob (
  hash            UUID PRIMARY KEY,
  stored_length   BIGINT NOT NULL,
  original_length BIGINT NOT NULL,
  loid            OID    NOT NULL
);

CREATE TABLE metadata (
  key     VARCHAR PRIMARY KEY,
  created TIMESTAMPTZ NOT NULL,
  hash    UUID        NULL,
  extra   VARCHAR     NULL
);
