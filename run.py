from pynvml import *

from subprocess import Popen, PIPE, STDOUT
import os
import sqlite3
conn = sqlite3.connect('kernels.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("SELECT * FROM Application;")
nvmlInit()

def format_name(str):
    return str.lower().replace(" ","_")

def get_device_name(idx):
    handle = nvmlDeviceGetHandleByIndex(idx)
    return nvmlDeviceGetName(handle)


for row in cursor.fetchall():
    print(os.environ['RODINIA_DIR'],row["binary"],row["parameters"])
    cmd = os.environ['RODINIA_DIR']+row["binary"]+" "+row["parameters"]
    nvprof_cmd = os.environ["CUDA_DIR"]+"bin/nvprof -o "+format_name(get_device_name(1))+"_"+format_name(row["title"])+".db" + " " + cmd
    p = Popen(nvprof_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=None,  close_fds=True)
    print p.stdout.read()

conn.close()
nvmlShutdown()
