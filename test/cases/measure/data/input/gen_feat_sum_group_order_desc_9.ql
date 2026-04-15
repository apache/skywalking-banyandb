SELECT id, entity_id, SUM(value), total::field, value::field FROM MEASURE service_cpm_minute IN sw_metric TIME > '-15m' GROUP BY id, value::field ORDER BY DESC
