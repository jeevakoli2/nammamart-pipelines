-- RAW layer: exact copy of the CSV, every column as text (empty string stays empty string),
-- plus lineage columns. Append-only: never cleaned, never updated, never deleted.
-- Template vars: {{table}}, {{path}}, {{source_file}}, {{run_id}}
CREATE TABLE IF NOT EXISTS {{table}} AS
SELECT *, CAST(NULL AS TIMESTAMP) AS _loaded_at, CAST(NULL AS VARCHAR) AS _source_file, CAST(NULL AS VARCHAR) AS _run_id
FROM read_csv('{{path}}', header = true, all_varchar = true, nullstr = '<<NULL>>', strict_mode = false)
WHERE false;

-- BY NAME: a renamed / missing column makes the load FAIL LOUDLY instead of shifting data into the wrong column
INSERT INTO {{table}} BY NAME
SELECT *, now()::TIMESTAMP AS _loaded_at, '{{source_file}}' AS _source_file, '{{run_id}}' AS _run_id
FROM read_csv('{{path}}', header = true, all_varchar = true, nullstr = '<<NULL>>', strict_mode = false);
