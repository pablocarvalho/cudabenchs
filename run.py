from pynvml import *

from subprocess import Popen, PIPE, STDOUT
import os
import sqlite3

device_idx = 1 # GTX 980

def format_name(str):
    return str.lower().replace(" ","_")

def get_device_name(idx):
    nvmlInit()
    handle = nvmlDeviceGetHandleByIndex(idx)
    name = nvmlDeviceGetName(handle)
    nvmlShutdown()
    return name

class ApplicationRunner:
    def __init__(self):
        self.connection = sqlite3.connect('kernels.db')
        self.connection.row_factory = sqlite3.Row

    def run(self):
        self.cursor = self.connection.cursor()
        self.cursor.execute("select * from Application as App inner join Benchmark as BM where App.benchmark = BM._id_")

        db_list = []

        for row in self.cursor.fetchall():
            print(os.environ[row['environment']],row["binary"],row["parameters"])
            cmd = os.environ[row['environment']] + row["binary"] + " " + row["parameters"]
            db_name = format_name(get_device_name(device_idx)) + "_" + format_name(row['name']) + " " + format_name(row["title"]) + ".db"
            nvprof_cmd = os.environ["CUDA_DIR"] + "bin/nvprof -o " + db_name + " " + cmd
            p = Popen(nvprof_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=None, close_fds=True)
            db_list.append((db_name,row['App._id_']))
            print p.stdout.read()

        self.cursor.close()
        return db_list

    def __del__(self):
        self.connection.close()

class KernelStorage:
    def __init__(self):
        self.connection = sqlite3.connect('kernels.db')
        self.connection.row_factory = sqlite3.Row

    def save(self):
        runner = ApplicationRunner()
        db_list = runner.run()

        for (db,app_id) in db_list:
            self.cursor.execute('select _id_ from Application where ')
            connection = sqlite3.connect(db)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute("select * from CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL as CK inner join StringTable as ST where ST._id_=CK.name;")

            self.cursor = self.connection.cursor()
            for row in cursor.fetchall():
                self.cursor.execute("insert into Kernels values (?,?,?,?,?,?,?,?,?,?,?,?)",(row['registersPerThread'],str(int(row['end'])-int(row['start'])),row['gridX'],row['gridY'],row['gridZ'],row['blockX'],row['blockY'],row['blockZ'],row['staticSharedMemory'],row['dynamicSharedMemory'],row['value'],app_id))
                self.connection.commit()
            self.cursor.close()

            cursor.close()
            connection.close()

    def __del__(self):
        self.connection.close()

storage = KernelStorage()
storage.save()
