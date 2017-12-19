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
import sqlite3

path = 'concurrent_logs/'

from random import randint

device_idx = 1 # GTX 980

import time

class CustomDBConnection:
    def __init__(self,path):
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.cursor = None

    def __del__(self):
        self.connection.close()

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

        cursor1.execute("select K.name,B.environment,App.binary,App.parameters from Kernels as K inner join Application as App on K.application = App._id_ and K.ranking<3 and K.cluster=1 inner join Benchmark as B on App.benchmark=B._id_;")
        cursor2.execute("select K.name,B.environment,App.binary,App.parameters from Kernels as K inner join Application as App on K.application = App._id_ and K.ranking<3 and K.cluster=2 inner join Benchmark as B on App.benchmark=B._id_;")

        rows1 = cursor1.fetchall()
        rows2 = cursor2.fetchall()

        start = 0;
        
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

                file_name = path+format_name(get_device_name(device_idx))+".output."
                

                prof = Runner("nvprof --profile-all-processes -o "+file_name+"%p.db",os.environ.copy())
                app1 = Runner(os.environ[row1['environment']] + row1["binary"] + " " + (row1["parameters"] or " "), env1)
                app2 = Runner(os.environ[row2['environment']] + row2["binary"] + " " + (row2["parameters"] or " "), env2)
                prof.start()
                time.sleep(2) #nvprof load overhead

                app1.start()
                time.sleep(1) #nvprof load overhead
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

           
                p = os.getcwd()+'/'+path
                print 'path: '+p

                list_files = [f for f in os.listdir(p) if f.endswith(".db")]

                files = sorted(list_files, key=lambda x: (x.split('.')[-2]))

                print files

                kernel1 = files[-2]
                kernel2 = files[-1]

                try:
                    if(start == 0):
                        self.evaluateConcurrency(p+kernel1,p+kernel2,row1['name'],row2['name'],False)
                    else:
                        self.evaluateConcurrency(p+kernel1,p+kernel2,row1['name'],row2['name'],True)
                    
                except Exception as e:
                    print str(e)

                os.rename(p+kernel1 , p+'histo/'+kernel1)
                os.rename(p+kernel2 , p+'histo/'+kernel2)

                start+=1

                
                
    """evaluateConcurrency crates a csv file named concurrencyEvaluation.csv where it stores comparison data from a pair of kernels using information of 
    single run of each kernel and information from both running together. The parameters are the sqlite files generated by nvprof 
    
    Args:
        app1concurrentBD (str): path to sqlite file that stores data from concurrent run of application 1 kernel
        app2concurrentBD (str): path to sqlite file that stores data from concurrent run of application 2 kernel
        ap1singleBD (str): path to sqlite file that stores data logs from the entire application where kernel 1 belongs
        ap2singleBD (str): path to sqlite file that stores data logs from the entire application where kernel 2 belongs
        accumulateData (bool) : if true it will just add a new line on a concurrencyEvaluation.csv
    """

    def evaluateConcurrency(self, app1concurrentBD,app2concurrentBD, kernel1Name, kernel2Name,accumulateData = True):

        outputTable = []



        #Used to get concurrent run databases
        print "name: "+app1concurrentBD
        print "name: "+app2concurrentBD
        concurrentDB1connection = CustomDBConnection(app1concurrentBD)
        cursorConcDB1 =  concurrentDB1connection.connection.cursor()
        cursorConcDB1.execute("SELECT start, end, completed, value as kernel_name FROM CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL AS CONCUR_KER INNER JOIN StringTable on CONCUR_KER.name = StringTable._id_ WHERE kernel_name = '"+kernel1Name+"';")
        resultConcDB1 = cursorConcDB1.fetchall()

        concurrentDB2connection = CustomDBConnection(app2concurrentBD)
        cursorConcDB2 =  concurrentDB2connection.connection.cursor()
        cursorConcDB2.execute("SELECT start, end, completed, value as kernel_name FROM CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL AS CONCUR_KER INNER JOIN StringTable on CONCUR_KER.name = StringTable._id_ WHERE kernel_name = '"+kernel2Name+"';")
        resultConcDB2 = cursorConcDB2.fetchall()

        resultConcDB1Size = len(resultConcDB1)
        resultConcDB2Size = len(resultConcDB2)

        #Verifications for data consistency
        if(resultConcDB1Size == 0):
            raise Exception("No data found on table CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL from "+app1concurrentBD + " kernel: " +kernel1Name)
        if(resultConcDB2Size == 0):
            raise Exception("No data found on table CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL from "+app2concurrentBD + " kernel: " +kernel2Name)
               
        #Used to get single run databases
        kernels_path = 'kernels.db'
        singleDBconnection = DBConnection()
        cursorSingleDB = singleDBconnection.connection.cursor()
        cursorSingleDB.execute("SELECT AVGTIME FROM KERNELS WHERE NAME = '"+kernel1Name+"';")
        resultSingleDB1 = cursorSingleDB.fetchall()        

        cursorSingleDB.execute("SELECT AVGTIME FROM KERNELS WHERE NAME = '"+kernel2Name+"';")
        resultSingleDB2 = cursorSingleDB.fetchall()        

        resultSingleDB1Size = len(resultSingleDB1)
        resultSingleDB2Size = len(resultSingleDB2)       

        limit = 0
        if(resultConcDB1Size < resultConcDB2Size ):
            limit = resultConcDB1Size
        else:
            limit = resultConcDB2Size
         
        i = 0

        kernel1namef = kernel1Name
        kernel1ConcStart = resultConcDB1[i]['start']
        kernel1ConcEnd = resultConcDB1[i]['end']
        kernel1ConcDur = kernel1ConcEnd - kernel1ConcStart
        kernel1SingStart = 0.0
        kernel1SingEnd = 0.0
        kernel1SingDur = resultSingleDB1[i]['avgTime']
        kernel1Loss = float(kernel1SingDur) / float(kernel1ConcDur)
        kernel1data = KernelData(kernel1namef,kernel1ConcStart,kernel1ConcEnd,kernel1ConcDur,kernel1SingStart,kernel1SingEnd,kernel1SingDur,kernel1Loss)

        print str(kernel1data)

        kernel2namef = kernel2Name
        kernel2ConcStart = resultConcDB2[i]['start']
        kernel2ConcEnd = resultConcDB2[i]['end']
        kernel2ConcDur = kernel2ConcEnd - kernel2ConcStart
        kernel2SingStart = 0.0
        kernel2SingEnd = 0.0
        kernel2SingDur = resultSingleDB1[i]['avgTime']
        kernel2Loss = float(kernel2SingDur) / float(kernel2ConcDur)
        kernel2data = KernelData(kernel2namef,kernel2ConcStart,kernel2ConcEnd,kernel2ConcDur,kernel2SingStart,kernel2SingEnd,kernel2SingDur,kernel2Loss)

        print str(kernel2data)

        
        #Verifies if both kernel really ran concurrently, if true it appends on output list
        if( (kernel1ConcStart >= kernel2ConcStart and kernel1ConcStart <= kernel2ConcEnd) or
            (kernel2ConcStart >= kernel1ConcStart and kernel2ConcStart <= kernel1ConcEnd) ):

            outputTable.append(OutputTuple(kernel1data,kernel2data))

        print "len: " +str(len(outputTable))
        self.writeTable(outputTable,accumulateData)

    def writeTable(self,outputTupleList, appendFile):
        if(appendFile == True):
            outputFile = open('concurrencyEvaluator.csv','a')
        else:
            outputFile = open('concurrencyEvaluator.csv','w')


        for i in range(len(outputTupleList)):
            #if(i == 0):
            #    outputFile.write(outputTupleList[i].getHeader());
            #    outputFile.write("\n");

            outputFile.write(str(outputTupleList[i]))
            outputFile.write("\n");

        outputFile.close();


class KernelData():
    def __init__(self, name, concurrent_start, concurrent_end, concurrent_duration, single_start, single_end, single_duration, lossPercentage):
        self.name = name
        self.concurrent_start = concurrent_start
        self.concurrent_end = concurrent_end
        self.concurrent_duration = concurrent_duration
        self.single_start = single_start
        self.single_end = single_end
        self.single_duration = single_duration
        self.lossPercentage = lossPercentage

    def __repr__(self):
        return self.name+';'+str(self.concurrent_start)+';'+str(self.concurrent_end)+';'+str(self.concurrent_duration)+';'+str(self.single_start)+';'+str(self.single_end)+';'+str(self.single_duration)+';'+str(self.lossPercentage)

    def getHeader(self):
        return "name;concurrent_start;concurrent_end;concurrent_duration;single_start;single_end;single_duration;loss_percentage"

class OutputTuple():

    def __init__(self, kernel1data, kernel2data):
        self.kernel1data = kernel1data;
        self.kernel2data = kernel2data;
        

    def __repr__(self):
        return str(self.kernel1data)+';'+str(self.kernel2data)

    def getHeader(self):
        return self.kernel1data.getHeader() + ";" + self.kernel2data.getHeader()

runner = ConcurrentRunner()
runner.run()
