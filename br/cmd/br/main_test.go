// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// Reference: https://dzone.com/articles/measuring-integration-test-coverage-rate-in-pouchc

import (
	"os"
	"strings"
	"testing"
)

func TestRunMain(*testing.T) {
	var args []string
	for _, arg := range os.Args {
		switch {
		case arg == "DEVEL":
		case strings.HasPrefix(arg, "-test."):
		default:
			args = append(args, arg)
		}
	}

	waitCh := make(chan struct{}, 1)

	os.Args = args
	go func() {
		main()
		close(waitCh)
	}()

	<-waitCh
}

func TestRunBackupRawMain(*testing.T) {
	args := []string{"test", "backup", "raw", "--help"}
	waitCh := make(chan struct{}, 1)

	os.Args = args
	go func() {
		main()
		close(waitCh)
	}()

	<-waitCh
}

func TestRunRestoreRawMain(*testing.T) {
	args := []string{"test", "restore", "raw", "--help"}
	waitCh := make(chan struct{}, 1)

	os.Args = args
	go func() {
		main()
		close(waitCh)
	}()

	<-waitCh
}
