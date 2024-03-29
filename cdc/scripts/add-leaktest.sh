#!/bin/sh
# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Usage: add-leaktest.sh pkg/*_test.go

set -eu

sed -i'~' -e '
  /^func (.*) Test.*(c \*check.C) {/ {
    n
    /testleak.AfterTest/! i\
		defer testleak.AfterTest(c)()
  }
' $@

for i in $@; do
	if ! cmp -s $i $i~; then
		gofumpt -w $i
	fi
	rm -f $i~
done

# Always report error when there are unstaged changes. So comment out.
# git --no-pager diff --exit-code
