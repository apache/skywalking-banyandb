-- Licensed to Apache Software Foundation (ASF) under one or more contributor
-- license agreements. See the NOTICE file distributed with
-- this work for additional information regarding copyright
-- ownership. Apache Software Foundation (ASF) licenses this file to you under
-- the Apache License, Version 2.0 (the "License"); you may
-- not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE TEMP TABLE samples(host text, v bigint);
INSERT INTO samples VALUES ('a',0),('a',2),('b',4),('b',NULL),('c',6),('c',NULL);
SELECT COALESCE(SUM(v),0), COALESCE(SUM(v),0)*2, SUM(COALESCE(v,0)+v), COUNT(DISTINCT(v+1)), SUM(v+1.5) FROM samples;
SELECT host, COALESCE(SUM(v),0)+COUNT(v) FROM samples GROUP BY host ORDER BY host;
SELECT COALESCE(SUM(v),0), COUNT(DISTINCT(v+1)) FROM samples WHERE false;
