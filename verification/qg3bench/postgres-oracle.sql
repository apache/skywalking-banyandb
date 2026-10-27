\pset null 'NULL'
SELECT version();
CREATE TEMP TABLE decimal_fixture(host text, ts integer, coefficient bigint);
INSERT INTO decimal_fixture SELECT 'a', i * 1000, 999999999999999999 FROM generate_series(0, 9) i;
INSERT INTO decimal_fixture VALUES ('b', 10000, -999999999999999999), ('b', 11000, -1), ('c', 12000, NULL);
CREATE TEMP VIEW decimal_values AS SELECT *, (coefficient::numeric / 10000000000)::numeric(38,10) AS value FROM decimal_fixture;
SELECT 'scalar', SUM(value)::numeric(38,10) FROM decimal_values;
SELECT 'grouped', host, SUM(value)::numeric(38,10), COUNT(value) FROM decimal_values GROUP BY host ORDER BY host;
SELECT 'having', host, SUM(value)::numeric(38,10) FROM decimal_values GROUP BY host HAVING SUM(value) > 0 ORDER BY host;
SELECT 'numeric_order', host, SUM(value)::numeric(38,10) AS total FROM decimal_values GROUP BY host ORDER BY total ASC NULLS LAST;
SELECT 'null_only', SUM(value)::numeric(38,10) FROM decimal_values WHERE ts >= 12000 AND ts < 13000;
SELECT 'empty', SUM(value)::numeric(38,10) FROM decimal_values WHERE ts >= 14000 AND ts < 15000;
SELECT 'cancellation', SUM(v)::numeric(38,10) FROM (VALUES (99999999.9999999999::numeric), (-99999999.9999999999::numeric)) t(v);
SELECT 'beyond_int64_coefficient', SUM(coefficient) > 9223372036854775807 FROM decimal_fixture WHERE host = 'a';
SELECT 'safe_mixed', SUM(value)::numeric(38,10), SUM(coefficient) FROM decimal_values WHERE ts >= 10000 AND ts < 12000;
SELECT 'source_cancellation', SUM(((coefficient - coefficient)::numeric / 10000000000))::numeric(38,10) FROM decimal_values;
