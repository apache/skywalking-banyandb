// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package run implements a lifecycle framework to control modules.
package run

import (
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"sync"

	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// FlagSet holds a pflag.FlagSet as well as an exported Name variable for
// allowing improved help usage information.
type FlagSet struct {
	*pflag.FlagSet
	Name string
}

// NewFlagSet returns a new FlagSet for usage in Config objects.
func NewFlagSet(name string) *FlagSet {
	return &FlagSet{
		FlagSet: pflag.NewFlagSet(name, pflag.ContinueOnError),
		Name:    name,
	}
}

// Unit is the default interface an object needs to implement for it to be able
// to register with a Group.
// Name should return a short but good identifier of the Unit.
type Unit interface {
	Name() string
}

// Role is an interface that should be implemented by Group Unit objects that
// need to be able to return their Role.
type Role interface {
	Role() databasev1.Role
}

// Config interface should be implemented by Group Unit objects that manage
// their own configuration through the use of flags.
// If a Unit's Validate returns an error it will stop the Group immediately.
type Config interface {
	// Unit for Group registration and identification
	Unit
	// FlagSet returns an object's FlagSet
	FlagSet() *FlagSet
	// Validate checks an object's stored values
	Validate() error
}

// PreRunner interface should be implemented by Group Unit objects that need
// a pre run stage before starting the Group Services.
// If a Unit's PreRun returns an error it will stop the Group immediately.
type PreRunner interface {
	// Unit for Group registration and identification
	Unit
	PreRun(context.Context) error
}

// StopNotify sends the stopped event to the running system.
type StopNotify <-chan struct{}

// Service interface should be implemented by Group Unit objects that need
// to run a blocking service until an error occurs or a shutdown request is
// made.
// The Serve method must be blocking and return an error on unexpected shutdown.
// Recoverable errors need to be handled inside the service itself.
// GracefulStop must gracefully stop the service and make the Serve call return.
//
// Since Service is managed by Group, it is considered a design flaw to call any
// of the Service methods directly in application code.
type Service interface {
	// Unit for Group registration and identification
	Unit
	// Serve starts the GroupService and blocks.
	Serve() StopNotify
	// GracefulStop shuts down and cleans up the GroupService.
	GracefulStop()
}

// Group builds on https://github.com/oklog/run to provide a deterministic way
// to manage service lifecycles. It allows for easy composition of elegant
// monoliths as well as adding signal handlers, metrics services, etc.
type Group struct {
	f            *FlagSet
	readyCh      chan struct{}
	log          *logger.Logger
	name         string
	r            run.Group
	c            []Config
	p            []PreRunner
	s            []Service
	rr           []Role
	showRunGroup bool
	configured   bool
}

// NewGroup return a Group with input name.
func NewGroup(name string) Group {
	return Group{
		name:    name,
		readyCh: make(chan struct{}),
	}
}

// Name shows the name of the group.
func (g Group) Name() string {
	return g.name
}

// Register will inspect the provided objects implementing the Unit interface to
// see if it needs to register the objects for any of the Group bootstrap
// phases. If a Unit doesn't satisfy any of the bootstrap phases it is ignored
// by Group.
// The returned array of booleans is of the same size as the amount of provided
// Units, signaling for each provided Unit if it successfully registered with
// Group for at least one of the bootstrap phases or if it was ignored.
func (g *Group) Register(units ...Unit) []bool {
	g.log = logger.GetLogger(g.name)
	hasRegistered := make([]bool, len(units))
	for idx := range units {
		if !g.configured {
			// if RunConfig has been called we can no longer register Config
			// phases of Units
			if c, ok := units[idx].(Config); ok {
				g.c = append(g.c, c)
				hasRegistered[idx] = true
			}
		}
		if p, ok := units[idx].(PreRunner); ok {
			g.p = append(g.p, p)
			hasRegistered[idx] = true
		}
		if s, ok := units[idx].(Service); ok {
			g.s = append(g.s, s)
			hasRegistered[idx] = true
		}
		if r, ok := units[idx].(Role); ok {
			g.rr = append(g.rr, r)
			hasRegistered[idx] = true
		}
	}
	return hasRegistered
}

// Deregister will remove the provided objects implementing the Unit interface
// from the Group.
// The returned array of booleans is of the same size as the amount of provided
// Units, signaling for each provided Unit if it successfully deregistered from
// Group or if it was ignored.
func (g *Group) Deregister(units ...Unit) []bool {
	hasDeregistered := make([]bool, len(units))
	for idx := range units {
		if c, ok := units[idx].(Config); ok {
			for i := range g.c {
				if g.c[i] == c {
					g.c[i] = nil
					hasDeregistered[idx] = true
				}
			}
		}
		if p, ok := units[idx].(PreRunner); ok {
			for i := range g.p {
				if g.p[i] == p {
					g.p[i] = nil
					hasDeregistered[idx] = true
				}
			}
		}
		if s, ok := units[idx].(Service); ok {
			for i := range g.s {
				if g.s[i] == s {
					g.s[i] = nil
					hasDeregistered[idx] = true
				}
			}
		}
		if r, ok := units[idx].(Role); ok {
			for i := range g.rr {
				if g.rr[i] == r {
					g.rr[i] = nil
					hasDeregistered[idx] = true
				}
			}
		}
	}
	return hasDeregistered
}

// RegisterFlags returns FlagSet contains Flags in all modules.
func (g *Group) RegisterFlags() *FlagSet {
	// run configuration stage
	g.f = NewFlagSet(g.name)
	g.f.SortFlags = false // keep order of flag registration
	g.f.Usage = func() {
		fmt.Printf("Flags:\n")
		g.f.PrintDefaults()
	}

	gFS := NewFlagSet("Common Service options")
	gFS.SortFlags = false
	gFS.StringVarP(&g.name, "name", "n", g.name, `name of this service`)
	gFS.BoolVar(&g.showRunGroup, "show-rungroup-units", false, "show rungroup units")
	g.f.AddFlagSet(gFS.FlagSet)

	// register flags from attached Config objects
	fs := make([]*FlagSet, len(g.c))
	for idx := range g.c {
		// a Namer might have been deregistered
		if g.c[idx] == nil {
			continue
		}
		g.log.Debug().Str("name", g.c[idx].Name()).Uint32("registered", uint32(idx+1)).Uint32("total", uint32(len(g.c))).Msg("register flags")
		fs[idx] = g.c[idx].FlagSet()
		if fs[idx] == nil {
			// no FlagSet returned
			g.log.Debug().Str("name", g.c[idx].Name()).Msg("config object did not return a flagset")
			continue
		}
		fs[idx].VisitAll(func(f *pflag.Flag) {
			if g.f.Lookup(f.Name) != nil {
				// log duplicate flag
				g.log.Warn().Str("name", f.Name).Uint32("registered", uint32(idx+1)).Msg("ignoring duplicate flag")
				return
			}
			g.f.AddFlag(f)
		})
	}
	return g.f
}

// RunConfig runs the Config phase of all registered Config aware Units.
// Only use this function if needing to add additional wiring between config
// and (pre)run phases and a separate PreRunner phase is not an option.
// In most cases it is best to use the Run method directly as it will run the
// Config phase prior to executing the PreRunner and Service phases.
// If an error is returned the application must shut down as it is considered
// fatal.
func (g *Group) RunConfig() (interrupted bool, err error) {
	g.log = logger.GetLogger(g.name)
	g.configured = true

	if g.name == "" {
		// use the binary name if custom name has not been provided
		g.name = path.Base(os.Args[0])
	}

	defer func() {
		if err != nil {
			g.log.Error().Err(err).Msg("unexpected exit")
		}
	}()

	// Load config from env and file
	if err = config.Load(g.f.Name, g.f.FlagSet); err != nil {
		return false, errors.Wrapf(err, "%s fails to load config", g.f.Name)
	}

	// bail early on help or version requests
	switch {
	case g.showRunGroup:
		fmt.Println(g.ListUnits())
		return true, nil
	}

	// Validate Config inputs
	for idx := range g.c {
		// a Config might have been deregistered during Run
		if g.c[idx] == nil {
			g.log.Debug().Uint32("ran", uint32(idx+1)).Msg("skipping validate")
			continue
		}
		g.log.Debug().Str("name", g.c[idx].Name()).Uint32("ran", uint32(idx+1)).Uint32("total", uint32(len(g.c))).Msg("validate config")
		if vErr := g.c[idx].Validate(); vErr != nil {
			err = multierr.Append(err, vErr)
		}
	}

	// exit on at least one Validate error
	if err != nil {
		return false, err
	}

	// log binary name and version
	g.log.Info().Msg("started")

	return false, nil
}

// Run will execute all phases of all registered Units and block until an error
// occurs.
// If RunConfig has been called prior to Run, the Group's Config phase will be
// skipped and Run continues with the PreRunner and Service phases.
//
// The following phases are executed in the following sequence:
//
//	Config phase (serially, in order of Unit registration)
//	  - FlagSet()        Get & register all FlagSets from Config Units.
//	  - Flag Parsing     Using the provided args (os.Args if empty)
//	  - Validate()       Validate Config Units. Exit on first error.
//
//	PreRunner phase (serially, in order of Unit registration)
//	  - PreRun(ctx context.Context)         Execute PreRunner Units. Exit on first error.
//
//	Service phase (concurrently)
//	  - Serve()          Execute all Service Units in separate Go routines.
//	  - Wait             Block until one of the Serve() methods returns
//	  - GracefulStop()   Call interrupt handlers of all Service Units.
//
//	Run will return with the originating error on:
//	- first Config.Validate()  returning an error
//	- first PreRunner.PreRun(ctx context.Context) returning an error
//	- first Service.Serve()    returning (error or nil)
//
//nolint:contextcheck
func (g *Group) Run(ctx context.Context) (err error) {
	// run config registration and flag parsing stages
	if interrupted, errRun := g.RunConfig(); interrupted || errRun != nil {
		return errRun
	}
	defer func() {
		if err != nil {
			g.log.Fatal().Err(err).Stack().Msg("unexpected exit")
		}
	}()
	rr := make([]databasev1.Role, 0, len(g.rr))
	for idx := range g.rr {
		if g.rr[idx] == nil || g.rr[idx].Role() == databasev1.Role_ROLE_UNSPECIFIED {
			continue
		}
		rr = append(rr, g.rr[idx].Role())
	}
	// Sort and deduplicate roles
	sort.Slice(rr, func(i, j int) bool {
		return rr[i] < rr[j]
	})
	deduplicateRoles := func(rr []databasev1.Role) []databasev1.Role {
		if len(rr) < 2 {
			return rr
		}
		// deduplicate roles
		rrDedup := make([]databasev1.Role, 0, len(rr))
		for i := 0; i < len(rr)-1; i++ {
			if rr[i] != rr[i+1] {
				rrDedup = append(rrDedup, rr[i])
			}
		}
		rrDedup = append(rrDedup, rr[len(rr)-1])
		return rrDedup
	}
	rr = deduplicateRoles(rr)

	g.log.Info().Interface("roles", rr).Msg("the node will run as")

	if ctx == nil {
		ctx = context.Background()
	}
	// execute pre run stage and exit on error
	for idx := range g.p {
		// a PreRunner might have been deregistered during Run
		if g.p[idx] == nil {
			continue
		}
		g.log.Debug().Uint32("ran", uint32(idx+1)).Uint32("total", uint32(len(g.p))).Str("name", g.p[idx].Name()).Msg("pre-run")
		if err := g.p[idx].PreRun(context.WithValue(ctx, common.ContextNodeRolesKey, rr)); err != nil {
			return err
		}
	}

	swg := &sync.WaitGroup{}
	swg.Add(len(g.s))
	go func() {
		swg.Wait()
		close(g.readyCh)
	}()
	// feed our registered services to our internal run.Group
	for idx := range g.s {
		// a Service might have been deregistered during Run
		s := g.s[idx]
		if s == nil {
			continue
		}

		g.log.Debug().Uint32("total", uint32(len(g.s))).Uint32("ran", uint32(idx+1)).Str("name", s.Name()).Msg("serve")
		g.r.Add(func() error {
			notify := s.Serve()
			swg.Done()
			<-notify
			return nil
		}, func(_ error) {
			g.log.Debug().Uint32("total", uint32(len(g.s))).Uint32("ran", uint32(idx+1)).Str("name", s.Name()).Msg("stop")
			s.GracefulStop()
		})
	}

	// start registered services and block
	return g.r.Run()
}

// ListUnits returns a list of all Group phases and the Units registered to each
// of them.
func (g Group) ListUnits() string {
	var (
		s string
		t = "cli"
	)

	if len(g.c) > 0 {
		s += "\n- config: "
		for _, u := range g.c {
			if u != nil {
				s += u.Name() + " "
			}
		}
	}
	if len(g.p) > 0 {
		s += "\n- prerun: "
		for _, u := range g.p {
			if u != nil {
				s += u.Name() + " "
			}
		}
	}
	if len(g.s) > 0 {
		s += "\n- serve : "
		for _, u := range g.s {
			if u != nil {
				t = "svc"
				s += u.Name() + " "
			}
		}
	}

	return fmt.Sprintf("Group: %s [%s]%s", g.name, t, s)
}

// WaitTillReady blocks the goroutine till all modules are ready.
func (g *Group) WaitTillReady() {
	<-g.readyCh
}
