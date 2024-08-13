QPS ?= 10

GROUP ?= "default"

.PHONY: up_traffic
up_traffic:
	curl -XPOST 'http://localhost:12800/mock-data/segments/tasks?qps=$(QPS)&group=$(GROUP)' -H'Content-Type: application/json' -d "@segment.tpl.json"

.PHONY: ls_traffic
ls_traffic:
	curl -XGET 'http://localhost:12800/mock-data/segments/tasks'

.PHONY: rm_traffic
rm_traffic:
	curl -XDELETE 'http://localhost:12800/mock-data/segments/tasks'