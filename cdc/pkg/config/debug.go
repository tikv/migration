// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import "github.com/pingcap/errors"

// DebugConfig represents config for ticdc unexposed feature configurations
type DebugConfig struct {
	// identify if the keyspan actor is enabled for keyspan pipeline
	// TODO: turn on after GA.
	EnableKeySpanActor bool `toml:"enable-keyspan-actor" json:"enable-keyspan-actor"`

	// EnableDBSorter enables db sorter.
	//
	// The default value is false.
	// TODO: turn on after GA.
	EnableDBSorter bool      `toml:"enable-db-sorter" json:"enable-db-sorter"`
	DB             *DBConfig `toml:"db" json:"db"`
}

// ValidateAndAdjust validates and adjusts the debug configuration
func (c *DebugConfig) ValidateAndAdjust() error {
	if err := c.DB.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}
	return nil
}
