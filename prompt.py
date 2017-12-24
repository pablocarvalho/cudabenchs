import os
import sys
import re
sys.path.append(os.path.realpath('.'))
from pprint import pprint

import inquirer

from pynvml import *

class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

nvmlInit()
print color.BOLD + "Driver Version:", nvmlSystemGetDriverVersion() + color.END

deviceCount = nvmlDeviceGetCount()
gpus = []

for i in range(deviceCount):
	handle = nvmlDeviceGetHandleByIndex(i)
	gpus.append((nvmlDeviceGetName(handle),str(i)))

questions = [inquirer.List('device_id',message="Choose a GPU",choices=gpus)]
answers = inquirer.prompt(questions)

pprint(answers)
