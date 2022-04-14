// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"strings"
)

// EncloseName formats name in sql.
func EncloseName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// EncloseDBAndTable formats the database and table name in sql.
func EncloseDBAndTable(database, table string) string {
	return fmt.Sprintf("%s.%s", EncloseName(database), EncloseName(table))
}
