-- Clean slate
DROP KEYSPACE IF EXISTS tcl;
CREATE KEYSPACE tcl WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE tcl;


-- 1. Line/Route level
CREATE TABLE delay_stats_line (
  window_ts timestamp,
  ligne text,
  direction text,
  total_passages bigint,
  late_count bigint,
  avg_delay_min float,
  early_count bigint,
  PRIMARY KEY ((ligne, direction), window_ts)
) WITH CLUSTERING ORDER BY (window_ts DESC);

-- 2. Stop level
CREATE TABLE delay_stats_stop (
  window_ts timestamp,
  ligne text,
  direction text,
  stop_id int,
  total_passages bigint,
  late_count bigint,
  avg_delay_min float,
  early_count bigint,
  PRIMARY KEY ((ligne, direction, stop_id), window_ts)
) WITH CLUSTERING ORDER BY (window_ts DESC);

CREATE TABLE tcl.delay_stats (
  window_ts  timestamp,
  ligne text,
  direction text,
  route_code text,
  pmr text,
  total_passages bigint,
  late_count bigint,
  avg_delay_min float,
  realtime_count bigint,
  PRIMARY KEY ((ligne, direction), window_ts)
) WITH CLUSTERING ORDER BY (window_ts DESC);
