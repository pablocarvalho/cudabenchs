#!/usr/bin/env python

from db import DBConnection
from utils import format_name, get_device_name
from pynvml import *
from subprocess import Popen, PIPE, STDOUT
import logging

log = logging.getLogger(__name__)
logging.basicConfig(filename='kernel-runner.log',level=logging.INFO, format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s", datefmt='%Y.%m.%d %H:%M:%S')

import os

import sqlite3
import subprocess

device_idx = 0 # GTX 980

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

storage = KernelStorage()
storage.save()
