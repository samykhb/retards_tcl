-- Force keyspace + table
DROP KEYSPACE IF EXISTS tcl;  -- clean slate
CREATE KEYSPACE tcl WITH replication = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
};

USE tcl;
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

