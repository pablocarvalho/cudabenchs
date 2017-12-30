#!/usr/bin/env python

from db import DBConnection
from utils import format_name, select_gpu, highlight_str
from subprocess import Popen, PIPE, STDOUT
import logging

log = logging.getLogger(__name__)
logging.basicConfig(filename='kernel-runner.log',level=logging.INFO, format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s", datefmt='%Y.%m.%d %H:%M:%S')

import os

import sqlite3
import subprocess

gpu_selected = select_gpu()
device = gpu_selected[1]['device_id']
print highlight_str("Device selected: " + gpu_selected[0])

class ApplicationRunner(DBConnection):
    def run(self):
        self.cursor = self.connection.cursor()
        self.cursor.execute("SELECT * FROM Application as App INNER JOIN Benchmark as BM WHERE App.benchmark = BM._id_")

        db_list = []

        for row in self.cursor.fetchall():
            print "Profiling "+row['name']+" Kernel: "+row["acronym"]
            cmd = os.environ[row['environment']] + row["binary"] + " " + (row["parameters"] or " ")
            db_name = format_name(gpu_selected[0]) + "_" + format_name(row['name']) + "_" + format_name(row["acronym"]) + ".db"

            cur_path = os.getcwd()
            os.chdir(os.environ[row['environment']] + row["binary"][:-len(row["binary"].split('/')[-1])])

            nvprof_cmd = os.environ["CUDA_DIR"] + "/bin/nvprof -o " + cur_path + "/" + db_name + " " + cmd
            log.info("Calling: "+nvprof_cmd)
            _env = os.environ.copy()
            _env['CUDA_VISIBLE_DEVICES'] = str(device)
            p = Popen(nvprof_cmd, env=_env,shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
            output, errors = p.communicate()

            os.chdir(cur_path)

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
            cursor.execute("""SELECT count(*) AS invocations,
                                (end-start) AS actualTime,
                                avg(end-start) AS avgTime,
                                min(end-start) AS minTime,
                                max(end-start) AS maxTime,
                                *
                            FROM CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL AS CK INNER JOIN StringTable AS ST 
                            WHERE ST._id_=CK.name GROUP BY value;""")

            self.cursor = self.connection.cursor()
            rows = cursor.fetchall()
            print highlight_str("Loading "+str(len(rows))+" kernel(s)..")

            for row in rows:
                try:
                    self.cursor.execute("""INSERT INTO Kernels(registersPerThread,actualTime,
                                            invocations,avgTime,minTime,maxTime,gridX,gridY,gridZ,
                                            blockX,blockY,blockZ,staticSharedMemory,dynamicSharedMemory,
                                            mangledName,application)
                                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                        (row['registersPerThread'], row['actualTime'],
                                         row['invocations'], row['avgTime'],
                                         row['minTime'], row['maxTime'],
                                         row['gridX'], row['gridY'], row['gridZ'], row['blockX'],
                                         row['blockY'], row['blockZ'], row['staticSharedMemory'],
                                         row['dynamicSharedMemory'], row['value'], app_id))
                    self.connection.commit()
                except sqlite3.Error as er:
                    log.warning(er.message + ", kernel" + row['value'])
            self.cursor.close()

            cursor.close()
            connection.close()

storage = KernelStorage()
storage.save()
