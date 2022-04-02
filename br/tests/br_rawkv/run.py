#!/usr/bin/env python3
#!coding:utf-8

import re
import argparse
import subprocess
import traceback


class rawkvTester:
    def __init__(self, global_args):
        self.pd = global_args.pd
        self.br = global_args.br
        self.helper = global_args.helper
        self.local_backup_dir = "/tmp/backup_restore_test/"


    def test_full_rawkv(self):
        checksum_empty = self._get_checksum("31", "3130303030303030")

        # prepare data
        self._run_cmd(self.helper,
                "-pd", self.pd,
                "-mode", "rand-gen",
                "-start-key", "31",
                "-end-key", "3130303030303030",
                "-duration", "10")
        self._run_cmd(self.helper,
                "-pd", self.pd,
                "-mode", "put",
                "-put-data", "311121:31, 31112100:32, 311122:33, 31112200:34, 3111220000:35, 311123:36")
        checksum_origin = self._get_checksum("31", "3130303030303030")
        checksum_partial = self._get_checksum("311111", "311122")

        start_key, end_key = "00", "ff"
        checksum = self._get_checksum(start_key, end_key)

        # backup
        self._run_cmd(self.br,
                "--pd", self.pd,
                "--check-requirements", "false",
                "backup", "raw",
                "-s", self.local_backup_dir,
                "--start", "31",
                "--end", "3130303030303030",
                "--format", "hex",
                "--concurrency", "4",
                "--crypter.method", "aes128-ctr",
                "--crypter.key", "0123456789abcdef0123456789abcdef")

        # clean data
        self._run_cmd(self.helper,
                "-pd", self.pd,
                "-mode", "delete",
                "-start-key", "31",
                "-end-key", "3130303030303030")

        checksum_new = self._get_checksum("31", "3130303030303030")

        if checksum_new != checksum_empty:
            self._exit_with_error("failed to delete data in range [{}, {}), checksum mismatch:\n  expected: {}\n  actual: {}".format("31", "3130303030303030", checksum_empty, checksum_new))

        # partial restore
        self._run_cmd(self.br,
                "--pd", self.pd,
                "--check-requirements", "false",
                "restore", "raw",
                "-s", self.local_backup_dir,
                "--start", "31311111",
                "--end", "311311122",
                "--format", "hex",
                "--concurrency", "4",
                "--crypter.method", "aes128-ctr",
                "--crypter.key", "0120123456789abcdef0123456789abcdef")

        # self._run_cmd(self.helper,
        #         "-pd", self.pd,
        #         "-mode", "scan",
        #         "-start-key", "311311121",
        #         "-end-key", "33",
        #         )
        checksum_new = self._get_checksum("31", "3130303030303030")
        if checksum_new != checksum_partial:
            self._exit_with_error("checksum failed after restore:\n  expected: {}\n  actual: {}".format(checksum_partial, checksum_new))

        print(checksum)


    def _get_checksum(self, start_key, end_key):
        output = self._run_cmd(self.helper,
                "-pd", self.pd,
                "-mode", "checksum",
                "-start-key", start_key,
                "-end-key", end_key)
        matched = re.search("Checksum result: .*", output)
        if matched:
            return str(matched.group(0))[len("Checksum result: "):]
        else:
            self._exit_with_error("get checksum failed:\n  start_key: {}\n  end_key: {}", start_key, end_key)


    def _run_cmd(self, cmd, *args):
        # construct command and arguments
        cmd_list = [cmd]
        for arg in args:
            cmd_list.append(arg)

        # execute the command, record erorr
        p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        output, error = p.communicate()

        # report error and exit if execute command failed
        if len(error) > 0:
            self._exit_with_error("EXECUTE COMMAND FAILED:\n  COMMAND: {}\n  ERROR: {}".format(' '.join(map(str, cmd_list)), error))

        # return output
        return output


    def _exit_with_error(self, error):
        print("traceback:")
        for line in traceback.format_stack():
            print(line.strip())

        print("\nerror:")
        print(error)
        exit(1)


def main():
    args = parse_args()
    tester = rawkvTester(args)
    tester.test_full_rawkv()


def parse_args():
    parser = argparse.ArgumentParser(description="The backup/restore integration test runner for RawKV")
    parser.add_argument(
            "--br",
            dest = "br",
            default = "../../bin/br",
            help = "The br binary to be tested. Default: '../../bin/br'")
    parser.add_argument(
            "--pd",
            dest = "pd",
            default = "127.0.0.1:2379",
            help = "The pd address of the TiKV cluster to be tested. Default: '127.0.0.1:2379'")
    parser.add_argument(
            "--test-helper",
            dest = "helper",
            default = "./rawkv",
            help = "The test helper binary to be used to populate and clean data for the TiKV cluster. Default: './rawkv'")
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
