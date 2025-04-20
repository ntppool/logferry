package main

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
)

func NewWriterMetrics(writer *kafka.Writer, registry prometheus.Registerer, subsystem string) {
	wm := writerMetrics{
		registry:  registry,
		subsystem: subsystem,
		writer:    writer,
	}

	wm.batchTime = wm.newSummary("batch_time", "kafka batch time")
	wm.batchBytes = wm.newSummary("batch_bytes", "kafka batch bytes")
	wm.batchQueueTime = wm.newSummary("batch_queue_time", "kafka batch queue time")
	wm.writeTime = wm.newSummary("write_time", "kafka write time")
	wm.waitTime = wm.newSummary("wait_time", "kafka wait time")
	wm.messages = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: wm.subsystem,
		Name:      "messages",
		Help:      "kafka messages",
	})
	wm.errors = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: wm.subsystem,
		Name:      "errors",
		Help:      "kafka errors",
	})
	registry.MustRegister(wm.messages, wm.errors)

	go func() {
		for {
			wm.updateStats()
			time.Sleep(1 * time.Second)
		}
	}()
}

type writerMetrics struct {
	subsystem string
	registry  prometheus.Registerer
	writer    *kafka.Writer

	batchTime, batchBytes, batchQueueTime, writeTime, waitTime *summary

	messages, errors prometheus.Counter
}

func (wm *writerMetrics) updateStats() {
	ws := wm.writer.Stats()

	wm.batchTime.add(ws.BatchTime.Count, ws.BatchTime.Sum.Seconds())
	wm.batchQueueTime.add(ws.BatchQueueTime.Count, ws.BatchQueueTime.Sum.Seconds())
	wm.waitTime.add(ws.WaitTime.Count, ws.WaitTime.Sum.Seconds())
	wm.writeTime.add(ws.WriteTime.Count, ws.WriteTime.Sum.Seconds())

	wm.batchBytes.add(ws.BatchBytes.Count, float64(ws.BatchBytes.Sum))

	wm.messages.Add(float64(ws.Messages))
	wm.errors.Add(float64(ws.Errors))
}

// summary is registered into Prometheus registrerer, see newSummary below.
type summary struct {
	desc *prometheus.Desc

	mu    sync.Mutex
	count uint64
	sum   float64
}

func (wm *writerMetrics) newSummary(name, help string) *summary {
	if len(wm.subsystem) > 0 {
		name = wm.subsystem + "_" + name
	}
	s := &summary{
		desc: prometheus.NewDesc(name, help, nil, nil),
	}
	wm.registry.MustRegister(s)
	return s
}

func (s *summary) add(count int64, sum float64) {
	s.mu.Lock()
	s.count += uint64(count)
	s.sum += sum
	s.mu.Unlock()
}

func (s *summary) Describe(descs chan<- *prometheus.Desc) {
	descs <- s.desc
}

func (s *summary) Collect(metrics chan<- prometheus.Metric) {
	s.mu.Lock()
	count, sum := s.count, s.sum
	s.mu.Unlock()

	metrics <- prometheus.MustNewConstSummary(s.desc, count, sum, nil)
}
