#!/usr/bin/env python3
#!coding:utf-8

import re
import sys
import argparse
import subprocess
import traceback
import os


class rawkvTester:
    # storage_path_suffix: used for separating backup storages for different test cases.
    def __init__(self, global_args, storage_path_suffix=None, failpoints=''):
        storage = global_args.br_storage if storage_path_suffix is None \
                    else os.path.join(global_args.br_storage, storage_path_suffix)

        self.pd = global_args.pd
        self.br = global_args.br
        self.helper = global_args.helper
        self.br_storage = storage
        self.failpoints = failpoints


    def test_rawkv(self):
        outer_start, outer_end = "31", "3130303030303030"
        inner_start, inner_end = "311111", "311122"
        self._clean_range(outer_start, outer_end) # clean data left by previous test case.
        cs_outer_empty = self._get_checksum(outer_start, outer_end)

        # prepare and backup data
        self._randgen(outer_start, outer_end)
        self._run_cmd(self.helper, "-pd", self.pd, "-mode", "put",
                "-put-data", "311121:31, 31112100:32, 311122:33, 31112200:34, 3111220000:35, 311123:36")
        self._backup_range(outer_start, outer_end)
        cs_outer_origin = self._get_checksum(outer_start, outer_end)
        cs_inner_origin = self._get_checksum(inner_start, inner_end)

        # clean and restore outer range
        self._clean_range(outer_start, outer_end)
        cs_outer_clean = self._get_checksum(outer_start, outer_end)
        self._assert("clean range failed, checksum mismatch.\n  actual: {}\n  expect: {}", cs_outer_clean, cs_outer_empty)
        self._restore_range(outer_start, outer_end)
        cs_outer_restore = self._get_checksum(outer_start, outer_end)
        self._assert("restore failed, checksum mismatch.\n  actual: {}\n  expect: {}", cs_outer_restore, cs_outer_origin)

        # clean and restore inner range
        self._clean_range(outer_start, outer_end)
        cs_outer_clean = self._get_checksum(outer_start, outer_end)
        self._assert("clean range failed, checksum mismatch.\n  actual: {}\n  expect: {}", cs_outer_clean, cs_outer_empty)
        self._restore_range(inner_start, inner_end)
        cs_inner_restore = self._get_checksum(inner_start, inner_end)
        self._assert("restore failed, checksum mismatch.\n  actual: {}\n  expect: {}", cs_inner_restore, cs_inner_origin)


    def _backup_range(self, start_key, end_key):
        env = {
            'GO_FAILPOINTS': self.failpoints,
        }
        self._run_cmd(self.br, "--pd", self.pd, "backup", "raw", "-s", self.br_storage,
                "--start", start_key, "--end", end_key, "--format", "hex", "--check-requirements=false",
                "-L", "debug", **env)


    def _restore_range(self, start_key, end_key):
        env = {
            'GO_FAILPOINTS': self.failpoints,
        }
        self._run_cmd(self.br, "--pd", self.pd, "restore", "raw", "-s", self.br_storage,
                "--start", start_key, "--end", end_key, "--format", "hex", "--check-requirements=false",
                "-L", "debug", **env)


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
            self._exit_with_error("get checksum failed:\n  start_key: {}\n  end_key: {}", start_key, end_key)


    def _run_cmd(self, cmd, *args, **env):
        # construct command and arguments
        cmd_list = cmd.split() # `cmd` may contain arguments, so split() to meet requirement of `subprocess.check_output`.
        for arg in args:
            cmd_list.append(arg)

        output = subprocess.check_output(cmd_list, stderr=sys.stderr, universal_newlines=True, env=env)
        return str(output)


    def _assert(self, fmt, actual, expect):
        if actual != expect:
            self._exit_with_error(fmt.format(actual, expect))


    def _exit_with_error(self, error):
        print("traceback:")
        for line in traceback.format_stack():
            print(line.strip())

        print("\nerror:\n{}".format(error))
        exit(1)


def main():
    args = parse_args()

    tester = rawkvTester(args, storage_path_suffix="basic")
    tester.test_rawkv()

    # ingest "region error" to trigger fineGrainedBackup
    tester_fp = rawkvTester(args, storage_path_suffix="fp",
        failpoints='github.com/tikv/migration/br/pkg/backup/tikv-region-error=return("region error")')
    tester_fp.test_rawkv()


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
