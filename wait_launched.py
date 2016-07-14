#!/usr/bin/env python
import os
import re
import sys
import time
import os.path

class TarantoolLog(object):
    def __init__(self, path):
        self.path = path
        self.log_begin = 0

    def positioning(self):
        if os.path.exists(self.path):
            with open(self.path, 'r') as f:
                f.seek(0, os.SEEK_END)
                self.log_begin = f.tell()
        return self

    def seek_once(self, msg):
        if not os.path.exists(self.path):
            return -1
        with open(self.path, 'r') as f:
            f.seek(self.log_begin, os.SEEK_SET)
            while True:
                log_str = f.readline()

                if not log_str:
                    return -1
                pos = log_str.find(msg)
                if pos != -1:
                    return pos

    def seek_wait(self, msg, proc=None):
        while True:
            if os.path.exists(self.path):
                break
            time.sleep(0.001)

        with open(self.path, 'r') as f:
            f.seek(self.log_begin, os.SEEK_SET)
            cur_pos = self.log_begin
            while True:
                if not (proc is None):
                    if not (proc.poll() is None):
                        raise OSError("Can't start Tarantool")
                log_str = f.readline()
                if not log_str:
                    time.sleep(0.001)
                    f.seek(cur_pos, os.SEEK_SET)
                    continue
                if re.findall(msg, log_str):
                    return
                cur_pos = f.tell()

path = os.path.dirname(sys.argv[0])
if not path:
    path = '.'
os.chdir(path)

if sys.argv.count > 1:
    name = 'var/%s.log' % sys.argv[1]
    log = TarantoolLog(name).positioning()
    log.seek_wait('ready to accept requests')
    sys.exit(0)

sys.exit(1)
