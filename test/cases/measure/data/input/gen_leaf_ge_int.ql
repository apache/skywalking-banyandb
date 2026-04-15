SELECT id, service_id, name, short_name, service_group, layer FROM MEASURE service_traffic IN index_mode TIME > '-15m' WHERE layer >= 2
