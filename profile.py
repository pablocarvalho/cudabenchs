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

            nvprof_cmd = os.environ["CUDA_DIR"] + "/bin/nvprof -f -o " + cur_path + "/" + db_name + " " + cmd
            log.info("Calling: "+nvprof_cmd)
            _env = os.environ.copy()
            _env['CUDA_VISIBLE_DEVICES'] = str(device)
            _env['CUDA_DEVICE_ORDER'] = 'PCI_BUS_ID' #CUDA VERSION >=7.0
            p = Popen(nvprof_cmd, env=_env,shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
            output, errors = p.communicate()

            os.chdir(cur_path)

            if p.returncode: #or errors:
                print errors
            else:
                db_list.append((db_name, row['_id_']))

        self.cursor.close()
        return db_list

class KernelStorage(DBConnection):
    def save(self):
        num_iterations = 30

        for num_iteration in range(num_iterations):
            print highlight_str("#Iteration: " + str(num_iteration))

            runner = ApplicationRunner()
            db_list = runner.run()
            print "Loading " + str(len(db_list)) + " database(s).."

            for (db, app_id) in db_list:
                print "Connecting to DB: " + db
                connection = sqlite3.connect(db)
                connection.row_factory = sqlite3.Row
                cursor = connection.cursor()
                cursor.execute("""SELECT count(*) AS invocations,
                                    (end-start) AS actualTime,
                                    avg(end-start) AS avgTimeApp,
                                    min(end-start) AS minTimeApp,
                                    max(end-start) AS maxTimeApp,
                                    *
                                FROM CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL AS CK INNER JOIN StringTable AS ST 
                                WHERE ST._id_=CK.name GROUP BY value;""")

                nvprof_rows = cursor.fetchall()

                print highlight_str("Loading " + str(len(nvprof_rows)) + " kernel(s)..")

                for nvprof_row in nvprof_rows:
                    self.cursor = self.connection.cursor()
                    self.cursor.execute("SELECT _id_ FROM Kernels WHERE mangledName = ?;", (nvprof_row['value'],))
                    app_rows = self.cursor.fetchall()

                    if(len(app_rows) == 0):
                        self.cursor.execute("""INSERT INTO Kernels(registersPerThread,invocations,
                                                    avgTimeApp,minTimeApp,maxTimeApp,gridX,gridY,gridZ,
                                                    blockX,blockY,blockZ,staticSharedMemory,dynamicSharedMemory,
                                                    mangledName,application)
                                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                                (nvprof_row['registersPerThread'], nvprof_row['invocations'],
                                                nvprof_row['avgTimeApp'], nvprof_row['minTimeApp'], nvprof_row['maxTimeApp'],
                                                nvprof_row['gridX'], nvprof_row['gridY'], nvprof_row['gridZ'], nvprof_row['blockX'],
                                                nvprof_row['blockY'], nvprof_row['blockZ'], nvprof_row['staticSharedMemory'],
                                                nvprof_row['dynamicSharedMemory'], nvprof_row['value'], app_id))
                        self.connection.commit()

                    self.cursor.execute("SELECT * FROM Kernels WHERE mangledName = ?", (nvprof_row['value'],))
                    old_target_row = self.cursor.fetchone()

                    cursor.execute("""SELECT (end-start) AS actualTime,* FROM CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL
                                    AS CK INNER JOIN StringTable AS ST 
                                    WHERE ST._id_=CK.name and value = ?;""", (nvprof_row['value'],))
                
                    target_row = cursor.fetchone()

                    self.cursor.execute("""UPDATE Kernels SET minTimeFirst = ?,
                                            maxTimeFirst = ?, avgTimeFirst = ?
                                            WHERE mangledName = ?;""",
                                        (min(old_target_row['minTimeFirst'] if old_target_row['minTimeFirst'] is not None else float('+inf'), target_row['actualTime']),
                                        max(old_target_row['maxTimeFirst'] if old_target_row['maxTimeFirst'] is not None else float('-inf'), target_row['actualTime']),
                                        int(old_target_row['avgTimeFirst'] if old_target_row['avgTimeFirst'] is not None else 0) + int(target_row['actualTime'])/float(num_iterations),
                                        nvprof_row['value']))
                    self.connection.commit()
                self.cursor.close()

                cursor.close()
                connection.close()

storage = KernelStorage()
storage.save()
