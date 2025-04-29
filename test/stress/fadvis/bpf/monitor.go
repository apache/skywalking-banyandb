package bpf

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"os"
	"os/signal"

	"github.com/cilium/ebpf/perf"
)

type Event struct {
	Pid  uint32
	Ts   uint64
	Comm [16]byte
}

func MonitorFDChanges(ctx context.Context) {
	rd, err := perf.NewReader(/* map from BPF object */, os.Getpagesize())
	if err != nil {
		log.Printf("failed to create perf reader: %v", err)
		return
	}
	defer rd.Close()

	for {
		select {
		case <-ctx.Done():
			log.Println("[eBPF] Monitor stopped")
			return
		default:
			record, err := rd.Read()
			if err != nil {
				continue
			}
			if record.LostSamples > 0 {
				log.Printf("lost %d samples\n", record.LostSamples)
				continue
			}

			var e Event
			if err := binary.Read(bytes.NewBuffer(record.RawSample), binary.LittleEndian, &e); err != nil {
				log.Printf("decode failed: %v", err)
				continue
			}

			log.Printf("[eBPF] fadvise: pid=%d, ts=%d, comm=%s\n", e.Pid, e.Ts, e.Comm)
		}
	}
}