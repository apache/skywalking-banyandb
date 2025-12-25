package ktm

import (
	"sort"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/metrics"
	fodcmetrics "github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
)

// ToRawMetrics converts KTM metrics to FODC RawMetrics.
func ToRawMetrics(store *metrics.Store) []fodcmetrics.RawMetric {
	if store == nil {
		return nil
	}

	allMetrics := store.GetAll()
	var rawMetrics []fodcmetrics.RawMetric

	for _, metricSet := range allMetrics {
		for _, m := range metricSet.GetMetrics() {
			rm := fodcmetrics.RawMetric{
				Name:   m.Name,
				Value:  m.Value,
				Desc:   m.Help,
				Labels: make([]fodcmetrics.Label, 0, len(m.Labels)),
			}

			// Convert labels
			for k, v := range m.Labels {
				rm.Labels = append(rm.Labels, fodcmetrics.Label{
					Name:  k,
					Value: v,
				})
			}

			// Sort labels for consistency
			sort.Slice(rm.Labels, func(i, j int) bool {
				return rm.Labels[i].Name < rm.Labels[j].Name
			})

			rawMetrics = append(rawMetrics, rm)
		}
	}

	return rawMetrics
}
