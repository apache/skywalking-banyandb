package detector

import (
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/poller"
)

type AlertManager struct {
	errorRateThreshold float64
	alerts             []Alert
}

type Alert struct {
	Message   string
	Timestamp time.Time
	Severity  string
}

func NewAlertManager(errorRateThreshold float64) *AlertManager {
	return &AlertManager{
		errorRateThreshold: errorRateThreshold,
		alerts:             []Alert{},
	}
}

func (a *AlertManager) AnalyzeMetrics(snapshot poller.MetricsSnapshot) []string {
	var alerts []string

	// Build a map of metrics by name for easier lookup
	metricsMap := make(map[string]float64)
	for _, m := range snapshot.RawMetrics {
		// Use metric name as key (could also use full key with labels)
		metricsMap[m.Name] = m.Value
	}

	// Check for high error rates
	if errRate, ok := metricsMap["banyandb_liaison_grpc_total_failed"]; ok {
		if total, ok := metricsMap["banyandb_liaison_grpc_total_started"]; ok && total > 0 {
			rate := errRate / total
			if rate > a.errorRateThreshold {
				msg := fmt.Sprintf("High error rate detected: %.2f%% (threshold: %.2f%%)", rate*100, a.errorRateThreshold*100)
				alerts = append(alerts, msg)
				a.alerts = append(a.alerts, Alert{
					Message:   msg,
					Timestamp: snapshot.Timestamp,
					Severity:  "critical",
				})
			}
		}
	}

	// Check memory usage
	if memUsed, ok := metricsMap["banyandb_system_memory_state"]; ok {
		if memTotal, ok := metricsMap["banyandb_system_memory_total"]; ok && memTotal > 0 {
			usage := memUsed / memTotal
			if usage > 0.9 {
				msg := fmt.Sprintf("High memory usage: %.2f%%", usage*100)
				alerts = append(alerts, msg)
				a.alerts = append(a.alerts, Alert{
					Message:   msg,
					Timestamp: snapshot.Timestamp,
					Severity:  "warning",
				})
			}
		}
	}

	// Check disk usage
	if diskUsed, ok := metricsMap["banyandb_system_disk"]; ok {
		if diskTotal, ok := metricsMap["banyandb_system_disk_total"]; ok && diskTotal > 0 {
			usage := diskUsed / diskTotal
			if usage > 0.8 {
				msg := fmt.Sprintf("High disk usage: %.2f%%", usage*100)
				alerts = append(alerts, msg)
				a.alerts = append(a.alerts, Alert{
					Message:   msg,
					Timestamp: snapshot.Timestamp,
					Severity:  "warning",
				})
			}
		}
	}

	// Check for zero write rate (might indicate failure)
	if writeRate, ok := metricsMap["banyandb_measure_total_written"]; ok {
		if writeRate == 0 {
			// Check if this is consistently zero (would need historical data)
			// For now, just log if we see it
		}
	}

	// Check for connection failures
	if connFailures, ok := metricsMap["banyandb_connection_failures_total"]; ok {
		if connFailures > 10 {
			msg := fmt.Sprintf("High connection failure count: %.0f", connFailures)
			alerts = append(alerts, msg)
			a.alerts = append(a.alerts, Alert{
				Message:   msg,
				Timestamp: snapshot.Timestamp,
				Severity:  "critical",
			})
		}
	}

	// Check for any errors in the snapshot
	if len(snapshot.Errors) > 0 {
		for _, err := range snapshot.Errors {
			msg := fmt.Sprintf("Metrics polling error: %s", err)
			alerts = append(alerts, msg)
			a.alerts = append(a.alerts, Alert{
				Message:   msg,
				Timestamp: snapshot.Timestamp,
				Severity:  "warning",
			})
		}
	}

	return alerts
}

func (a *AlertManager) HandleDeathRattle(event DeathRattleEvent) {
	alert := Alert{
		Message:   fmt.Sprintf("Death rattle (%s): %s", event.Type, event.Message),
		Timestamp: event.Timestamp,
		Severity:  event.Severity,
	}
	a.alerts = append(a.alerts, alert)
}

func (a *AlertManager) GetRecentAlerts(duration time.Duration) []Alert {
	cutoff := time.Now().Add(-duration)
	var recent []Alert
	for _, alert := range a.alerts {
		if alert.Timestamp.After(cutoff) {
			recent = append(recent, alert)
		}
	}
	return recent
}

