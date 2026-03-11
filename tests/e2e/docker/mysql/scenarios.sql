USE dbcheck;

TRUNCATE TABLE no_pk_big_table;
TRUNCATE TABLE full_scan_case;
TRUNCATE TABLE myisam_case;
TRUNCATE TABLE redundant_index_case;
TRUNCATE TABLE many_index_case;
TRUNCATE TABLE wide_composite_index_case;

INSERT INTO no_pk_big_table (name, payload)
SELECT CONCAT('name-', seq_data.n),
       RPAD(CONCAT('payload-', seq_data.n), 180, 'x')
FROM (
  SELECT @scenario_row_no_pk := @scenario_row_no_pk + 1 AS n
  FROM information_schema.columns, (SELECT @scenario_row_no_pk := 0) init
  LIMIT 800
) AS seq_data;

INSERT INTO full_scan_case (id, category, payload)
SELECT seq_scan.n,
       IF(MOD(seq_scan.n, 2) = 0, 'hot', 'cold'),
       RPAD(CONCAT('scan-', seq_scan.n), 120, 'y')
FROM (
  SELECT @scenario_row_scan := @scenario_row_scan + 1 AS n
  FROM information_schema.columns, (SELECT @scenario_row_scan := 0) init
  LIMIT 1400
) AS seq_scan;

INSERT INTO myisam_case (id, value_text)
SELECT id, CONCAT('m-', id) FROM full_scan_case WHERE id <= 300;

INSERT INTO redundant_index_case (name, note)
SELECT CONCAT('name-', seq_idx.n),
       CONCAT('note-', seq_idx.n)
FROM (
  SELECT @scenario_row_idx := @scenario_row_idx + 1 AS n
  FROM information_schema.columns, (SELECT @scenario_row_idx := 0) init
  LIMIT 200
) AS seq_idx;

INSERT INTO many_index_case (c1, c2, c3, c4, c5, c6, c7, note)
SELECT seq_mix.n,
       seq_mix.n + 1,
       seq_mix.n + 2,
       seq_mix.n + 3,
       seq_mix.n + 4,
       seq_mix.n + 5,
       seq_mix.n + 6,
       CONCAT('many-', seq_mix.n)
FROM (
  SELECT @scenario_row_many := @scenario_row_many + 1 AS n
  FROM information_schema.columns, (SELECT @scenario_row_many := 0) init
  LIMIT 150
) AS seq_mix;

INSERT INTO wide_composite_index_case (c1, c2, c3, c4, c5, note)
SELECT CONCAT('k', seq_comp.n),
       CONCAT('a', MOD(seq_comp.n, 5)),
       CONCAT('b', MOD(seq_comp.n, 7)),
       CONCAT('c', MOD(seq_comp.n, 11)),
       CONCAT('d', MOD(seq_comp.n, 13)),
       CONCAT('wide-', seq_comp.n)
FROM (
  SELECT @scenario_row_comp := @scenario_row_comp + 1 AS n
  FROM information_schema.columns, (SELECT @scenario_row_comp := 0) init
  LIMIT 160
) AS seq_comp;

INSERT INTO backup_history (backup_type, backup_time, backup_size_mb, is_valid)
VALUES
  ('full', NOW() - INTERVAL 12 HOUR, 12288, 1),
  ('incremental', NOW() - INTERVAL 6 HOUR, 3584, 1);

SELECT COUNT(*) FROM no_pk_big_table WHERE payload LIKE 'payload-%';
SELECT COUNT(*) FROM no_pk_big_table WHERE name LIKE 'name-%';
SELECT COUNT(*) FROM full_scan_case WHERE category = 'hot';
SELECT SQL_NO_CACHE SUM(LENGTH(payload)) FROM full_scan_case WHERE payload LIKE '%scan-%';
SELECT category, COUNT(*) FROM full_scan_case GROUP BY category ORDER BY COUNT(*) DESC;
SELECT COUNT(*) FROM (
  SELECT payload FROM full_scan_case ORDER BY payload DESC LIMIT 300
) AS sample_payloads;

SELECT COUNT(*) FROM myisam_case WHERE value_text LIKE 'm-%';
SELECT COUNT(*) FROM redundant_index_case WHERE name LIKE 'name-%';
SELECT COUNT(*) FROM many_index_case WHERE c3 > 10;
SELECT COUNT(*) FROM wide_composite_index_case WHERE c5 LIKE 'd%';
SELECT COUNT(*) FROM auto_inc_case;
SELECT COUNT(*) FROM (
  SELECT SQL_BIG_RESULT category, COUNT(*) AS cnt
  FROM full_scan_case
  GROUP BY category
  ORDER BY cnt DESC
) AS grouped_category_counts;
SELECT COUNT(*) FROM (
  SELECT SQL_BIG_RESULT LEFT(payload, 12) AS prefix_key, COUNT(*) AS cnt
  FROM full_scan_case
  GROUP BY prefix_key
  ORDER BY cnt DESC
) AS grouped_prefix_counts;

SELECT SLEEP(0.12);
SELECT SLEEP(0.15);
SELECT SLEEP(0.2);
