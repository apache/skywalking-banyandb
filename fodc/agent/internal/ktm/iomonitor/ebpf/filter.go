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

//go:build linux

package ebpf

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cilium/ebpf"
	"golang.org/x/sys/unix"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf/generated"
)

func configureFilters(objs *generated.IomonitorObjects, cgroupPath string, targetComm string) error {
	targetPath, err := resolveTargetCgroupPath(cgroupPath)
	if err != nil {
		return fmt.Errorf("resolve target cgroup: %w", err)
	}

	cgID, err := getCgroupIDFromPath(targetPath)
	if err != nil {
		return fmt.Errorf("read cgroup id: %w", err)
	}

	if err := updateConfigMap(objs.ConfigMap, cgID); err != nil {
		return fmt.Errorf("program config map: %w", err)
	}

	pids, err := findPIDsByCommPrefix(targetComm)
	if err != nil {
		return fmt.Errorf("find pids: %w", err)
	}

	if err := replaceAllowedPIDs(objs.AllowedPids, pids); err != nil {
		return fmt.Errorf("program allowed pids: %w", err)
	}

	return nil
}

func refreshAllowedPIDs(objs *generated.IomonitorObjects, prefix string) error {
	pids, err := findPIDsByCommPrefix(prefix)
	if err != nil {
		return err
	}
	return replaceAllowedPIDs(objs.AllowedPids, pids)
}

func resolveTargetCgroupPath(cfgPath string) (string, error) {
	if cfgPath != "" {
		return resolveCgroupPath(cfgPath)
	}

	if path, err := detectBanyanDBCgroupPath(); err == nil {
		return path, nil
	}

	cgRel, err := readCgroupV2Path("self")
	if err != nil {
		return "", err
	}
	return filepath.Join("/sys/fs/cgroup", cgRel), nil
}

func updateConfigMap(configMap *ebpf.Map, cgroupID uint64) error {
	if configMap == nil {
		return fmt.Errorf("config_map not available")
	}

	key := uint32(0)
	return configMap.Update(key, cgroupID, ebpf.UpdateAny)
}

func replaceAllowedPIDs(pidMap *ebpf.Map, pids []uint32) error {
	if pidMap == nil {
		return fmt.Errorf("allowed_pids map not available")
	}

	desired := make(map[uint32]struct{}, len(pids))
	for _, pid := range pids {
		desired[pid] = struct{}{}
		val := uint8(1)
		if err := pidMap.Update(pid, val, ebpf.UpdateAny); err != nil {
			return fmt.Errorf("update allowed pid %d: %w", pid, err)
		}
	}

	iter := pidMap.Iterate()
	var pid uint32
	var flag uint8
	for iter.Next(&pid, &flag) {
		if _, ok := desired[pid]; !ok {
			_ = pidMap.Delete(pid)
		}
	}

	return iter.Err()
}

func findPIDsByCommPrefix(prefix string) ([]uint32, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("read /proc: %w", err)
	}

	var pids []uint32
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		pidVal, err := strconv.Atoi(ent.Name())
		if err != nil || pidVal <= 0 {
			continue
		}

		commBytes, err := os.ReadFile(filepath.Join("/proc", ent.Name(), "comm"))
		if err != nil {
			continue
		}
		comm := strings.TrimSpace(string(commBytes))
		if strings.HasPrefix(comm, prefix) {
			pids = append(pids, uint32(pidVal))
		}
	}

	return pids, nil
}

func getCgroupIDFromPath(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat cgroup path %s: %w", path, err)
	}

	stat, ok := info.Sys().(*unix.Stat_t)
	if !ok {
		return 0, fmt.Errorf("unexpected stat type for %s", path)
	}
	return stat.Ino, nil
}
