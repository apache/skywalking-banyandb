CREATE TEMP TABLE samples(host text, v bigint);
INSERT INTO samples VALUES ('a',0),('a',2),('b',4),('b',NULL),('c',6),('c',NULL);
SELECT COALESCE(SUM(v),0), COALESCE(SUM(v),0)*2, SUM(COALESCE(v,0)+v), COUNT(DISTINCT(v+1)), SUM(v+1.5) FROM samples;
SELECT host, COALESCE(SUM(v),0)+COUNT(v) FROM samples GROUP BY host ORDER BY host;
SELECT COALESCE(SUM(v),0), COUNT(DISTINCT(v+1)) FROM samples WHERE false;
