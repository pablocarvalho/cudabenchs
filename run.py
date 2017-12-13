from subprocess import Popen, PIPE, STDOUT
import os
import sqlite3
conn = sqlite3.connect('kernels.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("SELECT * FROM Application;")

for row in cursor.fetchall():
    print(os.environ['RODINIA_DIR'],row["binary"],row["parameters"])
    cmd = os.environ['RODINIA_DIR']+row["binary"]+" "+row["parameters"]
    nvprof_cmd = os.environ["CUDA_DIR"]+"bin/nvprof -o output_"+row["title"].lower().replace(" ", "_")+".db" + " " + cmd
    p = Popen(nvprof_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=None,  close_fds=True)
    print p.stdout.read()

conn.close()
