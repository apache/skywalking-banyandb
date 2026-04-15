SELECT id, service_id, name, last_ping, layer FROM MEASURE service_instance_traffic IN sw_metric TIME > '-15m' WHERE name MATCH ('nodea')
