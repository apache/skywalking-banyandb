// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build windows

package claude

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func configureProcessTree(command *exec.Cmd) {
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		return killProcessTree(command.Process)
	}
}

func killProcessTree(process *os.Process) error {
	if process == nil {
		return nil
	}
	taskkillCmd := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(process.Pid))
	taskkillOutput, taskkillErr := taskkillCmd.CombinedOutput()
	if taskkillErr == nil {
		return nil
	}
	fallbackErr := process.Kill()
	if errors.Is(fallbackErr, os.ErrProcessDone) {
		return nil
	}
	message := strings.TrimSpace(string(taskkillOutput))
	if fallbackErr != nil {
		return errors.Join(
			fmt.Errorf("failed to kill Claude process tree with taskkill (%s): %w", message, taskkillErr),
			fmt.Errorf("failed to kill Claude parent process: %w", fallbackErr),
		)
	}
	return fmt.Errorf("failed to kill Claude process tree with taskkill (%s); parent process was killed: %w", message, taskkillErr)
}
