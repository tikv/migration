#!/usr/bin/env python3
#!coding:utf-8
# Copyright 2022 TiKV Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import re
import argparse
import subprocess
import traceback


class rawkvTester:
    def __init__(self, global_args, failpoints=''):
        self.pd = global_args.pd
        self.br = global_args.br
        self.helper = global_args.helper
        self.br_storage = global_args.br_storage
        self.failpoints = failpoints


    def test_dst_apiv1(self):
        test_name = self.test_dst_apiv1.__name__
        storage_dir = self.br_storage + "/" + test_name

        self._clean(storage_dir)
        self._run_br_test("v1", storage_dir)
        self._success_msg(test_name)


    def test_dst_apiv1ttl(self):
        test_name = self.test_dst_apiv1ttl.__name__
        storage_dir = self.br_storage + "/" + test_name

        self._clean(storage_dir)
        self._run_br_test("v1ttl", storage_dir)
        self._success_msg(test_name)


    def test_dst_apiv2(self):
        test_name = self.test_dst_apiv2.__name__
        storage_dir = self.br_storage + "/" + test_name

        self._clean(storage_dir)
        self._run_br_test("v2", storage_dir)
        self._success_msg(test_name)

    def _clean(self, storage_dir):
        if storage_dir.startswith("local://"):
            local_dir = storage_dir[len("local://"):]
            self._run_cmd("rm", "-rf", local_dir)
            self._run_cmd("mkdir", "-p", local_dir)


    def _success_msg(self, case_name):
        print(f"PASSED: {case_name}")


    def _run_br_test(self, dst_api_version, storage_dir):
        outer_start, outer_end = "31", "3130303030303030"
        inner_start, inner_end = "311111", "311122"

        # clean the data range to be tested
        self._clean_range(outer_start, outer_end)
        cs_outer_empty = self._get_checksum(outer_start, outer_end)

        # prepare and backup data
        self._randgen(outer_start, outer_end)
        self._run_cmd(self.helper, "-pd", self.pd, "-mode", "put",
                "-put-data", "311121:31, 31112100:32, 311122:33, 31112200:34, 3111220000:35, 311123:36")
        self._backup_range(outer_start, outer_end, dst_api_version, storage_dir)
        cs_outer_origin = self._get_checksum(outer_start, outer_end)
        cs_inner_origin = self._get_checksum(inner_start, inner_end)

        # clean and restore outer range
        self._clean_range(outer_start, outer_end)
        cs_outer_clean = self._get_checksum(outer_start, outer_end)
        self._assert("clean range failed, checksum mismatch.\n  actual: {}\n  expect: {}", cs_outer_clean, cs_outer_empty)
        self._restore_range(outer_start, outer_end, dst_api_version, storage_dir)
        cs_outer_restore = self._get_checksum(outer_start, outer_end)
        self._assert("restore failed, checksum mismatch.\n  actual: {}\n  expect: {}", cs_outer_restore, cs_outer_origin)

        # clean and restore inner range
        self._clean_range(outer_start, outer_end)
        cs_outer_clean = self._get_checksum(outer_start, outer_end)
        self._assert("clean range failed, checksum mismatch.\n  actual: {}\n  expect: {}", cs_outer_clean, cs_outer_empty)
        self._restore_range(inner_start, inner_end, dst_api_version, storage_dir)
        cs_inner_restore = self._get_checksum(inner_start, inner_end)
        self._assert("restore failed, checksum mismatch.\n  actual: {}\n  expect: {}", cs_inner_restore, cs_inner_origin)


    def _backup_range(self, start_key, end_key, dst_api_version, storage_dir):
        env = {
            'GO_FAILPOINTS': self.failpoints,
        }
        self._run_cmd(self.br, "--pd", self.pd, "backup", "raw", "-s", storage_dir,
                "--start", start_key, "--end", end_key, "--format", "hex", "--dst-api-version", dst_api_version,
                "--check-requirements=false", "-L", "debug", **env)


    def _restore_range(self, start_key, end_key, dst_api_version, storage_dir):
        env = {
            'GO_FAILPOINTS': self.failpoints,
        }
        self._run_cmd(self.br, "--pd", self.pd, "restore", "raw", "-s", storage_dir,
                "--start", start_key, "--end", end_key, "--format", "hex",
                "--check-requirements=false", "-L", "debug", **env)


    def _randgen(self, start_key, end_key):
        self._run_cmd(self.helper, "-pd", self.pd, "-mode", "rand-gen", "-start-key", start_key, "-end-key", end_key, "-duration", "10")


    def _clean_range(self, start_key, end_key):
        self._run_cmd(self.helper, "-pd", self.pd, "-mode", "delete", "-start-key", start_key, "-end-key", end_key)


    def _get_checksum(self, start_key, end_key):
        output = self._run_cmd(self.helper, "-pd", self.pd, "-mode", "checksum", "-start-key", start_key, "-end-key", end_key)
        matched = re.search("Checksum result: .*", output)
        if matched:
            return str(matched.group(0))[len("Checksum result: "):]
        else:
            self._exit_with_error(f"get checksum failed:\n  start_key: {start_key}\n  end_key: {end_key}")


    def _run_cmd(self, cmd, *args, **env):
        # construct command and arguments
        cmd_list = cmd.split() # `cmd` may contain arguments, so split() to meet requirement of `subprocess.check_output`.
        for arg in args:
            cmd_list.append(arg)

        # CalledProcessError
        try:
            output = subprocess.run(cmd_list, env=env, universal_newlines=True, check=True, stdout=subprocess.PIPE).stdout
        except  subprocess.CalledProcessError as e:
            self._exit_with_error(f"run command failed:\n  cmd: {e.cmd}\n  stdout: {e.stdout}\n  stderr: {e.stderr}")

        return str(output)


    def _assert(self, fmt, actual, expect):
        if actual != expect:
            self._exit_with_error(fmt.format(actual, expect))


    def _exit_with_error(self, error):
        print("traceback:")
        for line in traceback.format_stack():
            print(line.strip())

        print(f"\nerror:\n{error}")
        exit(1)


FAILPOINTS = [
    # ingest "region error" to trigger fineGrainedBackup
    'github.com/tikv/migration/br/pkg/backup/tikv-region-error=return("region error")',
]

def main():
    args = parse_args()
    for failpoint in [''] + FAILPOINTS:
        tester = rawkvTester(args, failpoints=failpoint)
        # tester.test_dst_apiv1() // does not support v1ttl --> v1
        tester.test_dst_apiv1ttl()
        # v1ttl -> v2 is not supported in current TiKV, enable this after V1/V1ttl -> V2 is enabled.
        # tester.test_dst_apiv2()


def parse_args():
    parser = argparse.ArgumentParser(description="The backup/restore integration test runner for RawKV")
    parser.add_argument("--br", dest = "br", required = True,
            help = "The br binary to be tested.")
    parser.add_argument("--pd", dest = "pd", required = True,
            help = "The pd address of the TiKV cluster to be tested.")
    parser.add_argument("--br-storage", dest = "br_storage", default = "local:///tmp/backup_restore_test",
            help = "The url to store SST files of backup/resotre. Default: 'local:///tmp/backup_restore_test'")
    parser.add_argument("--test-helper", dest = "helper", default = "./rawkv",
            help = "The test helper binary to be used to populate and clean data for the TiKV cluster. Default: './rawkv'")
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
