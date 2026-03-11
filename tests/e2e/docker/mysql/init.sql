CREATE DATABASE IF NOT EXISTS dbcheck;
USE dbcheck;

CREATE TABLE IF NOT EXISTS heartbeat (
  id INT PRIMARY KEY AUTO_INCREMENT,
  note VARCHAR(128) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO heartbeat (note) VALUES ('docker-e2e-ready');

CREATE TABLE IF NOT EXISTS backup_history (
  id INT PRIMARY KEY AUTO_INCREMENT,
  backup_type VARCHAR(16) NOT NULL,
  backup_time DATETIME NOT NULL,
  backup_size_mb BIGINT NOT NULL,
  is_valid TINYINT(1) NOT NULL
);

INSERT INTO backup_history (backup_type, backup_time, backup_size_mb, is_valid) VALUES
  ('full', NOW() - INTERVAL 3 DAY, 10240, 1),
  ('incremental', NOW() - INTERVAL 2 DAY, 2048, 1),
  ('incremental', NOW() - INTERVAL 1 DAY, 3072, 1);

CREATE TABLE IF NOT EXISTS lock_wait_case (
  id INT PRIMARY KEY,
  note VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

INSERT INTO lock_wait_case (id, note) VALUES (1, 'ready')
ON DUPLICATE KEY UPDATE note = VALUES(note);

CREATE TABLE IF NOT EXISTS ddl_lock_case (
  id INT PRIMARY KEY AUTO_INCREMENT,
  note VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

INSERT INTO ddl_lock_case (note) VALUES ('seed')
ON DUPLICATE KEY UPDATE note = VALUES(note);

CREATE TABLE IF NOT EXISTS no_pk_big_table (
  name VARCHAR(64) NOT NULL,
  payload VARCHAR(255) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS full_scan_case (
  id INT NOT NULL,
  category VARCHAR(32) NOT NULL,
  payload VARCHAR(255) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS myisam_case (
  id INT NOT NULL,
  value_text VARCHAR(64) NOT NULL,
  KEY idx_value_text (value_text)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS auto_inc_case (
  id INT NOT NULL AUTO_INCREMENT,
  note VARCHAR(128) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

ALTER TABLE auto_inc_case AUTO_INCREMENT = 1600000000;

CREATE TABLE IF NOT EXISTS redundant_index_case (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(64) NOT NULL,
  note VARCHAR(128) NOT NULL,
  PRIMARY KEY (id),
  KEY idx_name (name),
  KEY idx_name_note (name, note)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS many_index_case (
  id INT NOT NULL AUTO_INCREMENT,
  c1 INT,
  c2 INT,
  c3 INT,
  c4 INT,
  c5 INT,
  c6 INT,
  c7 INT,
  note VARCHAR(64) NOT NULL,
  PRIMARY KEY (id),
  KEY idx_c1 (c1),
  KEY idx_c2 (c2),
  KEY idx_c3 (c3),
  KEY idx_c4 (c4),
  KEY idx_c5 (c5),
  KEY idx_c6 (c6),
  KEY idx_c7 (c7)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS wide_composite_index_case (
  id INT NOT NULL AUTO_INCREMENT,
  c1 VARCHAR(32) NOT NULL,
  c2 VARCHAR(32) NOT NULL,
  c3 VARCHAR(32) NOT NULL,
  c4 VARCHAR(32) NOT NULL,
  c5 VARCHAR(32) NOT NULL,
  note VARCHAR(64) NOT NULL,
  PRIMARY KEY (id),
  KEY idx_c1_c2_c3_c4_c5 (c1, c2, c3, c4, c5)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS wide_table_case (
  id BIGINT NOT NULL AUTO_INCREMENT,
  c01 VARCHAR(32), c02 VARCHAR(32), c03 VARCHAR(32), c04 VARCHAR(32), c05 VARCHAR(32),
  c06 VARCHAR(32), c07 VARCHAR(32), c08 VARCHAR(32), c09 VARCHAR(32), c10 VARCHAR(32),
  c11 VARCHAR(32), c12 VARCHAR(32), c13 VARCHAR(32), c14 VARCHAR(32), c15 VARCHAR(32),
  c16 VARCHAR(32), c17 VARCHAR(32), c18 VARCHAR(32), c19 VARCHAR(32), c20 VARCHAR(32),
  c21 VARCHAR(32), c22 VARCHAR(32), c23 VARCHAR(32), c24 VARCHAR(32), c25 VARCHAR(32),
  c26 VARCHAR(32), c27 VARCHAR(32), c28 VARCHAR(32), c29 VARCHAR(32), c30 VARCHAR(32),
  c31 VARCHAR(32), c32 VARCHAR(32), c33 VARCHAR(32), c34 VARCHAR(32), c35 VARCHAR(32),
  c36 VARCHAR(32), c37 VARCHAR(32), c38 VARCHAR(32), c39 VARCHAR(32), c40 VARCHAR(32),
  c41 VARCHAR(32), c42 VARCHAR(32), c43 VARCHAR(32), c44 VARCHAR(32), c45 VARCHAR(32),
  c46 VARCHAR(32), c47 VARCHAR(32), c48 VARCHAR(32), c49 VARCHAR(32), c50 VARCHAR(32),
  c51 VARCHAR(32), c52 VARCHAR(32),
  PRIMARY KEY (id)
) ENGINE=InnoDB;

INSERT INTO auto_inc_case (note) VALUES ('auto-inc-baseline');
INSERT INTO redundant_index_case (name, note) VALUES ('seed', 'baseline');
INSERT INTO many_index_case (c1, c2, c3, c4, c5, c6, c7, note) VALUES (1, 1, 1, 1, 1, 1, 1, 'baseline');
INSERT INTO wide_composite_index_case (c1, c2, c3, c4, c5, note) VALUES ('a', 'b', 'c', 'd', 'e', 'baseline');
INSERT INTO wide_table_case (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10)
VALUES ('w1', 'w2', 'w3', 'w4', 'w5', 'w6', 'w7', 'w8', 'w9', 'w10');

INSERT INTO no_pk_big_table (name, payload)
SELECT CONCAT('name-', seq_data.n),
       RPAD(CONCAT('payload-', seq_data.n), 180, 'x')
FROM (
  SELECT @row_no_pk := @row_no_pk + 1 AS n
  FROM information_schema.columns, (SELECT @row_no_pk := 0) init
  LIMIT 600
) AS seq_data;

INSERT INTO full_scan_case (id, category, payload)
SELECT seq_scan.n,
       IF(MOD(seq_scan.n, 2) = 0, 'hot', 'cold'),
       RPAD(CONCAT('scan-', seq_scan.n), 120, 'y')
FROM (
  SELECT @row_scan := @row_scan + 1 AS n
  FROM information_schema.columns, (SELECT @row_scan := 0) init
  LIMIT 1200
) AS seq_scan;

INSERT INTO myisam_case (id, value_text)
SELECT id, CONCAT('m-', id) FROM full_scan_case WHERE id <= 300;

GRANT ALL PRIVILEGES ON dbcheck.* TO 'checker'@'%';
GRANT SELECT, PROCESS, REPLICATION CLIENT, SHOW DATABASES ON *.* TO 'checker'@'%';
GRANT SELECT ON mysql.* TO 'checker'@'%';
GRANT SELECT ON performance_schema.* TO 'checker'@'%';
FLUSH PRIVILEGES;
