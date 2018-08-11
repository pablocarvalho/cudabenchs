#!/usr/bin/env python

import os
import time
from threading import Thread
import subprocess
import signal
import logging
from random import randint
from db import DBConnection
from utils import *
from subprocess import Popen, PIPE, STDOUT

LOG = logging.getLogger(__name__)
logging.basicConfig(filename='kernel-runner.log', level=logging.INFO,
                    format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s",
                    datefmt='%Y.%m.%d %H:%M:%S')

DEVICE_NAME, DEVICE_IDX = select_gpu()
print highlight_str("Device selected: " + DEVICE_NAME)

LOG_DIR = 'concurrent_logs/'

class Runner(Thread):
    def __init__(self, env, cmd, params=''):
        self.p = None
        self.stdout = None
        self.stderr = None
        Thread.__init__(self)
        self.cmd = cmd
        self.env = env
        self.params = params
        print "CMD: " + self.cmd + " " + self.params

    def run(self):
        """Launching thread.."""
        cwd_dir = os.path.dirname(self.cmd)
        self.p = subprocess.Popen(self.cmd + " " + self.params, cwd=None if cwd_dir == '' else cwd_dir, env=self.env, preexec_fn=os.setsid, shell=True, stdout=PIPE, stderr=PIPE)
        self.stdout, self.stderr = self.p.communicate()
    
    def quit(self):
        """Quitting profiler.."""
        os.killpg(os.getpgid(self.p.pid), signal.SIGTERM)

class ConcurrentRunner(DBConnection):
    def check_order(self, db1, db2, name1, name2):
        print db1 + " looking for: " + name1
        print db2 + " looking for: " + name2

        conn1 = DBConnection(db1)
        conn2 = DBConnection(db2)

        cursor1 = conn1.connection.cursor()
        cursor2 = conn2.connection.cursor()

        cursor1.execute("""SELECT start FROM CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL AS CK
                            INNER JOIN StringTable AS ST on CK.name = ST._id_
                            WHERE ST.value = ?;""", (name1,))

        cursor2.execute("""SELECT start FROM CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL AS CK
                            INNER JOIN StringTable AS ST on CK.name = ST._id_
                            WHERE ST.value = ?;""", (name2,))

        result1 = cursor1.fetchall()
        result2 = cursor2.fetchall()

        size1 = len(result1)
        size2 = len(result2)

        #Verifications for data consistency
        if size1 == 0:
            raise Exception("No data found on table CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL of kernel: " + name1)
        if size2 == 0:
            raise Exception("No data found on table CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL of kernel: " + name2)

        if result1[0]['start'] > result2[0]['start']:
            raise Exception("The invocations were performed in reverse order!, k1: " + name1 + ", k2: " + name2)

    def run(self):
        cursor1 = self.connection.cursor()
        cursor2 = self.connection.cursor()

        groups1 = [1,2,3,4]
        groups2 = [1,2,3,4]
        #groups1 = [2] 
        #groups2 = [4]       
        validationSteps = 5

        for i in groups1:            
            for j in groups2:
                cursor1.execute("""SELECT K.mangledName,B.environment,App.binary,App.parameters
                                FROM Kernels AS K INNER JOIN Application AS App ON K.application = App._id_
                                INNER JOIN Benchmark AS B ON App.benchmark=B._id_
                                WHERE K.cluster=""" + str(i) + """ AND App._id_ != 13 ORDER BY K.ranking LIMIT 15;""")

                cursor2.execute("""SELECT K.mangledName,B.environment,App.binary,App.parameters
                                FROM Kernels AS K INNER JOIN Application AS App ON K.application = App._id_
                                INNER JOIN Benchmark AS B ON App.benchmark=B._id_
                                WHERE K.cluster="""+ str(j) +""" AND App._id_ != 13 ORDER BY K.ranking LIMIT 15;""")

                '''cursor1.execute("""SELECT K.mangledName,B.environment,App.binary,App.parameters from Kernels
                                AS K INNER JOIN Application AS App ON K.application = App._id_ AND K._id_=115
                                INNER JOIN Benchmark AS B ON App.benchmark=B._id_;""")

                cursor2.execute("""SELECT K.mangledName,B.environment,App.binary,App.parameters from Kernels
                                AS K INNER JOIN Application AS App ON K.application = App._id_ AND K._id_=116
                                INNER JOIN Benchmark AS B ON App.benchmark=B._id_;""")'''

                rows1 = cursor1.fetchall()
                rows2 = cursor2.fetchall()                

                self.writeHeader(i,j)

                print "Got " + str(len(rows1)) + " <- -> " + str(len(rows2))
                for row1 in rows1:
                    for row2 in rows2:

                        stepsData = []
                        for step in range(1,validationSteps + 1):
                            print "Running: " + row1['mangledName'] + " with " + row2['mangledName'] + " -> try "+ str(step) + " of " + str(validationSteps)

                            env1 = os.environ.copy()
                            env2 = os.environ.copy()

                            env1['LD_PRELOAD'] = os.getcwd() + "/cuHook/libcuhook.so.0"
                            env2['LD_PRELOAD'] = os.getcwd() + "/cuHook/libcuhook.so.1"

                            env1['CU_HOOK_DEBUG'] = env2['CU_HOOK_DEBUG'] = '1'

                            seed = str(randint(1, 50000))
                            env1['RANDOM_SEED'] = env2['RANDOM_SEED'] = seed
                            print "SEED: " + seed

                            env1['KERNEL_NAME_0'] = row1['mangledName']
                            env2['KERNEL_NAME_1'] = row2['mangledName']

                            prof = Runner(os.environ.copy(), "nvprof --timeout 60", "--profile-all-processes -o " + LOG_DIR + format_name(DEVICE_NAME) + ".output.%p.db")
                            app1 = Runner(env1, os.environ[row1['environment']] + row1["binary"], (row1["parameters"] or " "))
                            app2 = Runner(env2, os.environ[row2['environment']] + row2["binary"], (row2["parameters"] or " "))
                            prof.start()
                            time.sleep(2) #nvprof load overhead

                            app1.start()
                            app2.start()

                            app1.join()
                            app2.join()

                            print app1.stdout
                            print app2.stdout
                            print app1.stderr
                            print app2.stderr

                            prof.quit()
                            prof.join()

                            print prof.stdout
                            print prof.stderr

                            time.sleep(3) #storing db overhead

                            path = os.getcwd() + "/" + LOG_DIR
                            print 'Path: ' + path

                            list_files = [f for f in os.listdir(path) if f.endswith(".db")]
                            files = sorted(list_files, key=lambda x: (x.split('.')[-2]))

                            #print files

                            pid1 = "output."+str(app1.p.pid + 2)+".db"
                            pid2 = "output."+str(app2.p.pid + 2)+".db"
                            print pid1
                            print pid2
                            kernel1 = [f for f in os.listdir(path) if f.endswith(pid1)]
                            kernel2 = [f for f in os.listdir(path) if f.endswith(pid2)]

                            found1 = False
                            found2 = False

                            if len(kernel1) > 0 :
                                kernel1 = kernel1[0]  
                                found1 = True                  

                            if len(kernel2) > 0 :
                                kernel2 = kernel2[0]
                                found2 = True                        
                            
                            if(found1 and not found2):
                                for file in files:
                                    if file != kernel1:
                                        kernel2 = file
                                        found2 = True

                            if(found2 and not found1):
                                for file in files:
                                    if file != kernel2:
                                        kernel1 = file
                                        found1 = True

                            if(not found1 or not found2):
                                continue

                            print kernel1
                            print kernel2

                            if (app1.p.returncode == -signal.SIGSEGV):                
                                print "\n the application owner of " + kernel1 +" returned segmentation fault"
                                os.rename(path + kernel1, path + 'histo/' + kernel1)
                                os.rename(path + kernel2, path + 'histo/' + kernel2)
                                continue
                            if (app2.p.returncode == -signal.SIGSEGV):
                                print "\n the application owner of " + kernel2 +" returned segmentation fault"
                                os.rename(path + kernel1, path + 'histo/' + kernel1)
                                os.rename(path + kernel2, path + 'histo/' + kernel2)
                                continue                           

                            try:
                                result = None
                                result = self.evaluateConcurrency(path + kernel1, path + kernel2, row1['mangledName'], row2['mangledName'])
                                if( result is not None):
                                    stepsData.append(result)                                    
                                
                            except KernelException as e:
                                
                                print e
                                #os.rename(path + kernel1, path + 'histo/' + kernel1)
                                #os.rename(path + kernel2, path + 'histo/' + kernel2)
                                os.remove(path + kernel1)
                                os.remove(path + kernel2)
                                continue

                            #os.rename(path + kernel1, path + 'histo/' + kernel1)
                            #os.rename(path + kernel2, path + 'histo/' + kernel2)
                            os.remove(path + kernel1)
                            os.remove(path + kernel2)

                        self.evaluateMultipleOutputTuples(stepsData,i,j,validationSteps)
                

                #self.check_order(path + 'histo/' + kernel1, path + 'histo/' + kernel2, row1['mangledName'], row2['mangledName'])

    def evaluateMultipleOutputTuples(self, outputTupleList, group1id, group2id, numberOfSteps):

        kernel1LossAVG = 0.0
        kernel2LossAVG = 0.0

        kernel1ConcDurationAVG = 0.0
        kernel2ConcDurationAVG = 0.0

        if outputTupleList:
            kernel1 = outputTupleList[0].kernel1data
            kernel2 = outputTupleList[0].kernel2data
            
            for outputTuple in outputTupleList:
                kernel1LossAVG += outputTuple.kernel1data.lossPercentage
                kernel2LossAVG += outputTuple.kernel2data.lossPercentage

                kernel1ConcDurationAVG += outputTuple.kernel1data.concurrent_duration
                kernel2ConcDurationAVG += outputTuple.kernel2data.concurrent_duration

            kernel1LossAVG = kernel1LossAVG/len(outputTupleList)
            kernel2LossAVG = kernel2LossAVG/len(outputTupleList)

            kernel1ConcDurationAVG = kernel1ConcDurationAVG/len(outputTupleList)
            kernel2ConcDurationAVG = kernel2ConcDurationAVG/len(outputTupleList)

            #nvprof uses cicles as time unit, every start and end time would be different so I'm disconsidering then
            newKernel1 = KernelData(kernel1.name,0.0,0.0,kernel1ConcDurationAVG,kernel1.single_start,kernel1.single_end,kernel1.single_duration,kernel1LossAVG)
            newKernel2 = KernelData(kernel2.name,0.0,0.0,kernel2ConcDurationAVG,kernel2.single_start,kernel2.single_end,kernel2.single_duration,kernel2LossAVG)

            newOutputTuple = OutputTuple(newKernel1,newKernel2)

            self.writeTable(newOutputTuple,group1id, group2id, len(outputTupleList), numberOfSteps)
                
    """evaluateConcurrency crates a csv file named concurrencyEvaluation.csv where it stores comparison data from a pair of kernels using information of 
    single run of each kernel and information from both running together. The parameters are the sqlite files generated by nvprof 
    
    Args:
        app1concurrentBD (str): path to sqlite file that stores data from concurrent run of application 1 kernel
        app2concurrentBD (str): path to sqlite file that stores data from concurrent run of application 2 kernel
        ap1singleBD (str): path to sqlite file that stores data logs from the entire application where kernel 1 belongs
        ap2singleBD (str): path to sqlite file that stores data logs from the entire application where kernel 2 belongs
        accumulateData (bool) : if true it will just add a new line on a concurrencyEvaluation.csv
    """

    def evaluateConcurrency(self, app1concurrentBD, app2concurrentBD, kernel1Name, kernel2Name):
        outputTable = []

        #Used to get concurrent run databases
        print "Name: " + app1concurrentBD
        print "Name: " + app2concurrentBD

        concurrentDB1connection = DBConnection(app1concurrentBD)
        cursorConcDB1 =  concurrentDB1connection.connection.cursor()
        cursorConcDB1.execute("SELECT start, end, completed, value as kernel_name FROM CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL AS CONCUR_KER INNER JOIN StringTable on CONCUR_KER.name = StringTable._id_ WHERE kernel_name = '"+kernel1Name+"';")
        resultConcDB1 = cursorConcDB1.fetchall()

        concurrentDB2connection = DBConnection(app2concurrentBD)
        cursorConcDB2 =  concurrentDB2connection.connection.cursor()
        cursorConcDB2.execute("SELECT start, end, completed, value as kernel_name FROM CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL AS CONCUR_KER INNER JOIN StringTable on CONCUR_KER.name = StringTable._id_ WHERE kernel_name = '"+kernel2Name+"';")
        resultConcDB2 = cursorConcDB2.fetchall()

        resultConcDB1Size = len(resultConcDB1)
        resultConcDB2Size = len(resultConcDB2)

        #Verifications for data consistency
        if resultConcDB1Size == 0:
            raise KernelException("No data found on table CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL from " + app1concurrentBD + " kernel: " + kernel1Name, 1)
        if resultConcDB2Size == 0:
            raise KernelException("No data found on table CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL from " + app2concurrentBD + " kernel: " + kernel2Name, 2)        

               
        #Used to get single run databases
        singleDBconnection = DBConnection()
        cursorSingleDB = singleDBconnection.connection.cursor()
        cursorSingleDB.execute("SELECT avgTimeFirst, codeName FROM KERNELS WHERE mangledName = '" + kernel1Name + "';")
        resultSingleDB1 = cursorSingleDB.fetchall()

        cursorSingleDB.execute("SELECT avgTimeFirst, codeName FROM KERNELS WHERE mangledName = '" + kernel2Name + "';")
        resultSingleDB2 = cursorSingleDB.fetchall()

        resultSingleDB1Size = len(resultSingleDB1)
        resultSingleDB2Size = len(resultSingleDB2)

        limit = 0
        if resultConcDB1Size < resultConcDB2Size:
            limit = resultConcDB1Size
        else:
            limit = resultConcDB2Size

        i = 0

        kernel1namef = resultSingleDB1[i]['codeName']
        kernel1ConcStart = resultConcDB1[i]['start']
        kernel1ConcEnd = resultConcDB1[i]['end']
        kernel1ConcDur = kernel1ConcEnd - kernel1ConcStart

        if(kernel1ConcDur == 0.0):
            raise KernelException("duration time 0 in concurrent execution from " + app1concurrentBD + " kernel: " + kernel1Name, 1)

        kernel1SingStart = 0.0
        kernel1SingEnd = 0.0
        kernel1SingDur = resultSingleDB1[i]['avgTimeFirst']
        kernel1Loss = float(kernel1SingDur) / float(kernel1ConcDur)
        kernel1data = KernelData(kernel1namef, kernel1ConcStart, kernel1ConcEnd, kernel1ConcDur, kernel1SingStart, kernel1SingEnd, kernel1SingDur, kernel1Loss)

        #print str(kernel1data)

        kernel2namef = resultSingleDB2[i]['codeName']
        kernel2ConcStart = resultConcDB2[i]['start']
        kernel2ConcEnd = resultConcDB2[i]['end']
        kernel2ConcDur = kernel2ConcEnd - kernel2ConcStart

        if(kernel2ConcDur == 0.0):
            raise KernelException("duration time 0 in concurrent execution from " + app2concurrentBD + " kernel: " + kernel2Name, 2)

        kernel2SingStart = 0.0
        kernel2SingEnd = 0.0
        kernel2SingDur = resultSingleDB2[i]['avgTimeFirst']
        kernel2Loss = float(kernel2SingDur) / float(kernel2ConcDur)
        kernel2data = KernelData(kernel2namef, kernel2ConcStart, kernel2ConcEnd, kernel2ConcDur, kernel2SingStart, kernel2SingEnd, kernel2SingDur, kernel2Loss)

        #print str(kernel2data)

        #Verifies if both kernel really ran concurrently, if true it appends on output list
        if((kernel1ConcStart >= kernel2ConcStart and kernel1ConcStart <= kernel2ConcEnd) or
           (kernel2ConcStart >= kernel1ConcStart and kernel2ConcStart <= kernel1ConcEnd)):
            #outputTable.append(OutputTuple(kernel1data, kernel2data))
            return OutputTuple(kernel1data, kernel2data)
        else:
            return None



        #print "len: " +str(len(outputTable))
        #self.writeTable(outputTable, accumulateData,group1id, group2id)

    def writeHeader(self,group1id, group2id):
        outputFile = open('concurrencyEvaluator_g'+str(group1id)+ '-g'+ str(group2id) +'.csv', 'w')        
        outputFile.write(OutputTuple.getHeader())
        outputFile.write(";valid_attempts//total_attempts")
        outputFile.write("\n")

    def writeTable(self, outputTuple, group1id, group2id,validAttempts, totalAttempts):
        outputFile = open('concurrencyEvaluator_g'+str(group1id)+ '-g'+ str(group2id) +'.csv', 'a')
        valid_attempts = ";"+str(validAttempts) + "//" +str(totalAttempts)
        outputFile.write(str(outputTuple))
        outputFile.write(valid_attempts)
        outputFile.write("\n")
        outputFile.close()

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
    @staticmethod
    def getHeader():
        return "name;concurrent_start;concurrent_end;concurrent_duration;single_start;single_end;single_duration;loss_percentage"

    


class OutputTuple():

    def __init__(self, kernel1data, kernel2data):
        self.kernel1data = kernel1data
        self.kernel2data = kernel2data

    def __repr__(self):
        return str(self.kernel1data) + ';' + str(self.kernel2data)
    @staticmethod
    def getHeader():
        return KernelData.getHeader() + ";" + KernelData.getHeader()
    

runner = ConcurrentRunner()
runner.run()
