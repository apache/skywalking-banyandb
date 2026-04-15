SELECT id, entity_id, total, value FROM MEASURE service_cpm_minute IN sw_metric TIME > '-15m' WHERE id IN ('svc1')
