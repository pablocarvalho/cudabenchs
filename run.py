from pynvml import *
import logging

log = logging.getLogger(__name__)
logging.basicConfig(filename='kernel-runner.log',level=logging.INFO, format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s", datefmt='%Y.%m.%d %H:%M:%S')

from subprocess import Popen, PIPE, STDOUT
import os
import sqlite3

import threading
import subprocess

device_idx = 1 # GTX 980

def format_name(str):
    return str.lower().replace(" ","_")

def get_device_name(idx):
    nvmlInit()
    handle = nvmlDeviceGetHandleByIndex(idx)
    name = nvmlDeviceGetName(handle)
    nvmlShutdown()
    return name

class DBConnection:
    def __init__(self):
        self.connection = sqlite3.connect('kernels.db')
        self.connection.row_factory = sqlite3.Row
        self.cursor = None

    def __del__(self):
        self.connection.close()

class ApplicationRunner(DBConnection):
    def run(self):
        self.cursor = self.connection.cursor()
        self.cursor.execute("select * from Application as App inner join Benchmark as BM where App.benchmark = BM._id_")

        db_list = []

        for row in self.cursor.fetchall():
            print "Profiling "+row['name']+" .."
            cmd = os.environ[row['environment']] + row["binary"] + " " + (row["parameters"] or " ")
            db_name = format_name(get_device_name(device_idx)) + "_" + format_name(row['name']) + "_" + format_name(row["title"]) + ".db"
            nvprof_cmd = os.environ["CUDA_DIR"] + "bin/nvprof -o " + db_name + " " + cmd
            log.info("Calling: "+nvprof_cmd)
            p = Popen(nvprof_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
            output, errors = p.communicate()

            if p.returncode: #or errors:
                print errors
            else:
                db_list.append((db_name,row['_id_']))

        self.cursor.close()
        return db_list

class KernelStorage(DBConnection):
    def save(self):
        runner = ApplicationRunner()
        db_list = runner.run()
        print "Loading "+str(len(db_list))+" database(s).."

        for (db,app_id) in db_list:
            print "Connecting to DB: "+db
            connection = sqlite3.connect(db)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute("select * from CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL as CK inner join StringTable as ST where ST._id_=CK.name;")

            self.cursor = self.connection.cursor()
            rows = cursor.fetchall()
            print "Loading "+str(len(rows))+" invocation(s).."
            for row in rows:
                try:
                    self.cursor.execute("""insert into Kernels(registersPerThread,avgTime,gridX,gridY,gridZ,
                                                    blockX,blockY,blockZ,staticSharedMemory,dynamicSharedMemory,
                                                    name,application) values (?,?,?,?,?,?,?,?,?,?,?,?)""",
                                        (row['registersPerThread'],str(int(row['end'])-int(row['start'])),
                                         row['gridX'],row['gridY'],row['gridZ'],row['blockX'],
                                         row['blockY'],row['blockZ'],row['staticSharedMemory'],
                                         row['dynamicSharedMemory'],row['value'],app_id))
                    self.connection.commit()
                except sqlite3.Error as er:
                    log.warning(er.message + ", kernel" + row['value'])
            self.cursor.close()

            cursor.close()
            connection.close()

#storage = KernelStorage()
#storage.save()

class Runner(threading.Thread):
    def __init__(self,cmd,env):
        self.stdout = None
        self.stderr = None
        threading.Thread.__init__(self)
        self.cmd = cmd
        self.env = env
        print "CMD: " + self.cmd
        #print self.env

    def run(self):
        p = subprocess.Popen(self.cmd, env=self.env, shell=True, stdout=PIPE, stderr=PIPE)
        self.stdout, self.stderr = p.communicate()

class ConcurrentRunner(DBConnection):
    def run(self):
        cursor1 = self.connection.cursor()
        cursor2 = self.connection.cursor()

        cursor1.execute("select K.name,B.environment,App.binary,App.parameters from Kernels as K inner join Application as App on K.application = App._id_ and K.ranking<6 and K.cluster=1 inner join Benchmark as B on App.benchmark=B._id_;")
        cursor2.execute("select K.name,B.environment,App.binary,App.parameters from Kernels as K inner join Application as App on K.application = App._id_ and K.ranking<6 and K.cluster=2 inner join Benchmark as B on App.benchmark=B._id_;")

        rows1 = cursor1.fetchall()
        rows2 = cursor2.fetchall()

        print "Got " + str(len(rows1)) + " <- -> " + str(len(rows2))
        for row1 in rows1:
            for row2 in rows2:
                print "Running: " + row1['name'] + " with " + row2['name']

                env1 = os.environ.copy()
                env2 = os.environ.copy()
                env1['LD_PRELOAD'] = "./cuHook/libcuhook.so.0"
                env2['LD_PRELOAD'] = "./cuHook/libcuhook.so.1"
                env1['CU_HOOK_DEBUG']=env2['CU_HOOK_DEBUG']='1'

                print "Running1: " + os.environ[row1['environment']] + row1["binary"] + " " + (row1["parameters"] or " ")
                print "Running2: " + os.environ[row2['environment']] + row2["binary"] + " " + (row2["parameters"] or " ")

                app1 = Runner(os.environ[row1['environment']] + row1["binary"] + " " + (row1["parameters"] or " "), env1)
                app2 = Runner(os.environ[row2['environment']] + row2["binary"] + " " + (row2["parameters"] or " "), env2)
                app1.start();
                app2.start();
                app1.join()
                app2.join()

                print app1.stdout
                print app2.stdout
                return

runner = ConcurrentRunner()
runner.run()
