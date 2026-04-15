SELECT TOP 2 value ASC, id, entity_id, MEAN(value), total::field, value::field FROM MEASURE service_cpm_minute IN sw_metric TIME > '-15m' WHERE id != 'svc3' GROUP BY id, value::field ORDER BY DESC
