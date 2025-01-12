CREATE TABLE IF NOT EXISTS kine
(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name INTEGER,
    created INTEGER,
    deleted INTEGER,
    create_revision INTEGER,
    prev_revision INTEGER,
    lease INTEGER,
    value BLOB,
    old_value BLOB
);

-- GetCurrentSQL
EXPLAIN QUERY PLAN SELECT *
FROM (
  SELECT (SELECT MAX(rkv.id) AS id FROM kine AS rkv),
         *
  FROM kine AS kv
  JOIN (
    SELECT MAX(mkv.id) AS id
    FROM kine AS mkv
    WHERE
    mkv.name LIKE 'bla%'
    AND mkv.name > 'bla'
    GROUP BY mkv.name
  ) AS maxkv
  ON maxkv.id = kv.id
  WHERE
  kv.deleted = 0 OR
  true
) AS lkv
ORDER BY lkv.name ASC;

CREATE INDEX IF NOT EXISTS kine_name_index ON kine (name);
CREATE INDEX IF NOT EXISTS kine_name_id_index ON kine (name,id);
CREATE INDEX IF NOT EXISTS kine_id_deleted_index ON kine (id,deleted);
CREATE INDEX IF NOT EXISTS kine_prev_revision_index ON kine (prev_revision);
CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (name, prev_revision);

-- GetRevisionAfterSQL
EXPLAIN QUERY PLAN SELECT *
FROM (
  SELECT *
  FROM kine AS kv
  JOIN (
	  SELECT MAX(mkv.id) AS id
	  FROM kine AS mkv
	  WHERE mkv.name LIKE 'bla%'
	  AND mkv.name > 'bla'
    AND mkv.id <= 1111
	  GROUP BY mkv.name
  ) AS maxkv
  ON maxkv.id = kv.id
  WHERE
  kv.deleted = 0 OR
  false
) AS lkv
ORDER BY lkv.name ASC;

SELECT *
FROM kine AS kv
WHERE
    kv.name LIKE 'bla%' AND
    kv.id > 1111
ORDER BY kv.id ASC;

