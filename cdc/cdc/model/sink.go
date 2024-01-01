// Copyright 2020 PingCAP, Inc.
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

package model

//go:generate msgp

// MqMessageType is the type of message
type MqMessageType int

// Use explicit values to avoid compatibility issues.
const (
	// MqMessageTypeUnknown is unknown type of message key
	MqMessageTypeUnknown MqMessageType = 0
	// MqMessageTypeRow is row type of message key
	// MqMessageTypeRow MqMessageType = 1
	// MqMessageTypeDDL is ddl type of message key
	// MqMessageTypeDDL MqMessageType = 2
	// MqMessageTypeResolved is resolved type of message key
	MqMessageTypeResolved MqMessageType = 3
	// MqMessageTypeKv is RawKV entry type of message key
	MqMessageTypeKv MqMessageType = 4
)
