package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// sampleLatencyBuckets are the upper bounds, in milliseconds, of the histogram
// kept for each decrypt session. They span from well under a healthy sample to
// the per-operation deadline, so a run can be read against decryptIOTimeout
// directly: everything piling into the low buckets means the deadline has room
// to come down, and anything in the top ones is a stall worth explaining.
var sampleLatencyBuckets = [...]time.Duration{
	500 * time.Microsecond,
	1 * time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	3 * time.Second,
}

// sampleLatency accumulates per-sample decrypt timings for one session. It is
// touched only from the session's own goroutine — the gRPC handler decrypts one
// sample at a time — so it needs no lock, and it allocates nothing per sample.
type sampleLatency struct {
	count   int64
	totalNS int64
	maxNS   int64
	buckets [len(sampleLatencyBuckets) + 1]int64
}

func (s *sampleLatency) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.count++
	s.totalNS += int64(d)
	if int64(d) > s.maxNS {
		s.maxNS = int64(d)
	}
	for i, bound := range sampleLatencyBuckets {
		if d <= bound {
			s.buckets[i]++
			return
		}
	}
	s.buckets[len(sampleLatencyBuckets)]++
}

// quantile returns the upper bound of the bucket the given quantile falls in.
// Bucketed, so it reads as "at or below", never as an exact figure; the final
// open-ended bucket reports as the max actually seen.
func (s *sampleLatency) quantile(q float64) time.Duration {
	if s.count == 0 {
		return 0
	}
	target := int64(float64(s.count) * q)
	if target < 1 {
		target = 1
	}
	var seen int64
	for i, bound := range sampleLatencyBuckets {
		seen += s.buckets[i]
		if seen >= target {
			return bound
		}
	}
	return time.Duration(s.maxNS)
}

func (s *sampleLatency) mean() time.Duration {
	if s.count == 0 {
		return 0
	}
	return time.Duration(s.totalNS / s.count)
}

// histogram renders the non-empty buckets, so a healthy session stays a short
// line and a pathological one shows where the tail went.
func (s *sampleLatency) histogram() string {
	parts := make([]string, 0, len(s.buckets))
	for i, count := range s.buckets {
		if count == 0 {
			continue
		}
		if i == len(sampleLatencyBuckets) {
			parts = append(parts, fmt.Sprintf(">%s:%d", sampleLatencyBuckets[len(sampleLatencyBuckets)-1], count))
			continue
		}
		parts = append(parts, fmt.Sprintf("<=%s:%d", sampleLatencyBuckets[i], count))
	}
	return strings.Join(parts, " ")
}

// log emits one line per finished session. Info rather than Debug: it is a
// single line per track per instance, and it is the only place the per-sample
// cost the decrypt deadline is sized against can actually be read.
func (s *sampleLatency) log(instanceID, adamID string) {
	if s.count == 0 {
		return
	}
	logrus.Infof(
		"decrypt sample latency instance=%s adam_id=%s samples=%d mean=%s p50<=%s p95<=%s p99<=%s max=%s deadline=%s | %s",
		instanceID, adamID, s.count,
		s.mean().Round(time.Microsecond),
		s.quantile(0.50), s.quantile(0.95), s.quantile(0.99),
		time.Duration(s.maxNS).Round(time.Microsecond),
		decryptIOTimeout,
		s.histogram(),
	)
}
