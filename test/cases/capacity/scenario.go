// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package capacity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// Scenario is one data model, with a data set far larger than the query memory of the node under test and a query whose
// limit the ladder raises.
type Scenario struct {
	// Write stores the data set: Total results the probe can return.
	Write func(conn *grpc.ClientConn, base time.Time)
	// Count returns how many of the results are stored and queryable, counting window by window so the count never
	// asks for more than the node can serve.
	Count func(ctx context.Context, conn *grpc.ClientConn, base time.Time) (int, error)
	// Probe runs the query of the scenario with the limit and reports what it returned.
	Probe func(ctx context.Context, conn *grpc.ClientConn, base time.Time, limit int) (Outcome, error)
	// Name names the scenario in reports.
	Name string
	// PoolBytes is the query pool the node under test gets; a query may reserve an eighth of it.
	PoolBytes uint64
	// Total is the number of results the data set holds.
	Total int
}

// refused maps a gRPC error to ErrExhausted when the node refused the query for lack of query memory. A message that is too
// large for the client carries the same status code, and says nothing about the node, so it stays an ordinary error.
func refused(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.ResourceExhausted && (strings.Contains(err.Error(), "query memory pool exhausted") ||
		strings.Contains(err.Error(), "memory budget exceeded")) {
		return fmt.Errorf("%w: %s", ErrExhausted, err.Error())
	}
	return err
}

// Expect is what a scenario must serve on one topology.
type Expect struct {
	// Floor is the fewest results the node must serve, the regression guard of the scenario.
	Floor int
}

// Climb writes the data set, waits until all of it is stored, climbs the ladder and checks what it found:
//
//   - every query that is served returns exactly the limit it was given, since the data set holds more;
//   - every query that is refused is refused with RESOURCE_EXHAUSTED, never with another error;
//   - the node serves at least a limit of 1, and the data set is larger than the capacity, or the ladder measured the data
//     and not the memory;
//   - the boundary holds: a limit a tenth below the capacity is served again, and one a tenth above it is refused;
//   - every byte the queries reserved is returned once they are done.
//
// The ladder is reported either way, so a run shows how much each data model returns on the memory it was given.
func Climb(conn *grpc.ClientConn, base time.Time, sc *Scenario, where string) (*Result, func(limit int) (Outcome, error)) {
	sc.Write(conn, base)
	gm.Eventually(func(innerGm gm.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		count, err := sc.Count(ctx, conn, base)
		innerGm.Expect(err).NotTo(gm.HaveOccurred())
		innerGm.Expect(count).To(gm.Equal(sc.Total), "every result must be stored before the ladder starts")
	}, 3*flags.EventuallyTimeout, time.Second).Should(gm.Succeed())

	probe := func(limit int) (Outcome, error) {
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		return sc.Probe(ctx, conn, base, limit)
	}
	reserved := vectorized.QueryMemoryStats().LiveReservedBytes
	result, err := Find(sc.Total, probe)
	report := fmt.Sprintf("%s [%s]: pool %d bytes, data set %d results\n%s", sc.Name, where, sc.PoolBytes, sc.Total, result.Table())
	g.AddReportEntry("capacity "+sc.Name+" ["+where+"]", report)
	fmt.Fprintln(g.GinkgoWriter, report)
	fmt.Println(report)
	gm.Expect(err).NotTo(gm.HaveOccurred(), report)

	for _, rung := range result.Rungs {
		if rung.Err == nil {
			gm.Expect(rung.Outcome.Returned).To(gm.Equal(min(rung.Limit, sc.Total)),
				"limit %d must return exactly that many results of a data set of %d", rung.Limit, sc.Total)
		}
	}
	gm.Expect(result.MaxLimit).To(gm.BeNumerically(">=", 1),
		"the node refuses even a limit of 1 on a data set of %d results: the query memory is spent on data the limit does not ask for\n%s", sc.Total, report)
	gm.Expect(result.Saturated).To(gm.BeFalse(), "the whole data set was served: the data set is too small to find the capacity\n%s", report)
	// The boundary is checked a tenth away from the ladder's: the memory a query is charged shifts by a few results from run
	// to run, so the exact limit that tipped over once may be served the next time.
	below := max(1, result.MaxLimit-result.MaxLimit/10)
	again, err := probe(below)
	gm.Expect(err).NotTo(gm.HaveOccurred(), "limit %d, below the capacity %d, must be served again", below, result.MaxLimit)
	gm.Expect(again.Returned).To(gm.Equal(below))
	above := result.MaxLimit + result.MaxLimit/10 + 1
	_, err = probe(above)
	gm.Expect(errors.Is(err, ErrExhausted)).To(gm.BeTrue(), "limit %d, above the capacity %d, must be refused, got %v", above, result.MaxLimit, err)
	// Releases to the transport complete a moment after a query returns.
	gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, 10*time.Second, 50*time.Millisecond).
		Should(gm.Equal(reserved), "the queries of the ladder left reserved memory behind\n%s", report)
	return result, probe
}

// Run is Climb and the floor of the scenario on the topology: the node must serve at least expect.Floor results.
func Run(conn *grpc.ClientConn, base time.Time, sc *Scenario, expect Expect, where string) *Result {
	result, _ := Climb(conn, base, sc, where)
	gm.Expect(result.MaxLimit).To(gm.BeNumerically(">=", expect.Floor),
		"%s must serve at least %d results on %d bytes of query pool\n%s", sc.Name, expect.Floor, sc.PoolBytes, result.Table())
	return result
}
