#!/usr/bin/env python
 
from db import DBConnection
from utils import format_name, get_device_name
from subprocess import Popen, PIPE, STDOUT
import logging

log = logging.getLogger(__name__)
logging.basicConfig(filename='kernel-runner.log',level=logging.INFO, format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s", datefmt='%Y.%m.%d %H:%M:%S')

import os

from threading import Thread
import subprocess
import signal

from random import randint

device_idx = 0 # GTX 980

import time

class Runner(Thread):
    def __init__(self,cmd,env):
        self.p = None
        self.stdout = None
        self.stderr = None
        Thread.__init__(self)
        self.cmd = cmd
        self.env = env
        print("CMD: " + self.cmd)

    def run(self):
        """Launching thread.."""
        self.p = subprocess.Popen(self.cmd, env=self.env, preexec_fn=os.setsid, shell=True, stdout=PIPE, stderr=PIPE)
        self.stdout, self.stderr = self.p.communicate()
    
    def quit(self):
        """Quitting profiler.."""
        os.killpg(os.getpgid(self.p.pid), signal.SIGTERM)

class ConcurrentRunner(DBConnection):
    def run(self):
        cursor1 = self.connection.cursor()
        cursor2 = self.connection.cursor()

        cursor1.execute("select K.name,B.environment,App.binary,App.parameters from Kernels as K inner join Application as App on K.application = App._id_ and K.ranking<2 and K.cluster=1 inner join Benchmark as B on App.benchmark=B._id_;")
        cursor2.execute("select K.name,B.environment,App.binary,App.parameters from Kernels as K inner join Application as App on K.application = App._id_ and K.ranking<2 and K.cluster=2 inner join Benchmark as B on App.benchmark=B._id_;")

        rows1 = cursor1.fetchall()
        rows2 = cursor2.fetchall()

        print("Got " + str(len(rows1)) + " <- -> " + str(len(rows2)))
        for row1 in rows1:
            for row2 in rows2:
                print("Running: " + row1['name'] + " with " + row2['name'])

                env1 = os.environ.copy()
                env2 = os.environ.copy()
                
                env1['LD_PRELOAD'] = "./cuHook/libcuhook.so.0"
                env2['LD_PRELOAD'] = "./cuHook/libcuhook.so.1"
                
                env1['CU_HOOK_DEBUG'] = env2['CU_HOOK_DEBUG'] = '1'

                seed = str(randint(1,50000))
                env1['RANDOM_SEED'] = env2['RANDOM_SEED'] = seed
                print("SEED: "+seed)

                env1['KERNEL_NAME_0'] = row1['name']
                env2['KERNEL_NAME_1'] = row2['name']

		prof = Runner("nvprof --profile-all-processes -o "+format_name(get_device_name(device_idx))+".output.%h.%p",os.environ.copy())
                app1 = Runner(os.environ[row1['environment']] + row1["binary"] + " " + (row1["parameters"] or " "), env1)
                app2 = Runner(os.environ[row2['environment']] + row2["binary"] + " " + (row2["parameters"] or " "), env2)
                prof.start()
                time.sleep(2) #nvprof load overhead

                app1.start()
                app2.start()

                app1.join()
                app2.join()

                print(app1.stdout)
                print(app2.stdout)
                print(app1.stderr)
                print(app2.stderr)

                prof.quit()
                prof.join()

                print(prof.stdout)
                print(prof.stderr)

runner = ConcurrentRunner()
runner.run()
