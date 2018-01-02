""" This module contains utilitary functions."""

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

def format_name(strf):
    return strf.lower().replace(" ", "_")

def highlight_str(strr):
    return color.BOLD + strr + color.END

def warning_str(strr):
    return color.YELLOW + strr + color.END

def select_gpu():
    nvmlInit()
    print highlight_str("Driver Version: " + nvmlSystemGetDriverVersion())

    device_count = nvmlDeviceGetCount()
    gpus = []

    print "Number of GPUs: " + str(device_count)
    for i in range(device_count):
        handle = nvmlDeviceGetHandleByIndex(i)
        gpus.append((nvmlDeviceGetName(handle), i))

    if device_count > 1:
        questions = [inquirer.List('device_id', message="Choose a GPU", choices=gpus)]
        selected_option = inquirer.prompt(questions)
        return (nvmlDeviceGetName(nvmlDeviceGetHandleByIndex(selected_option['device_id'])),selected_option)

    return (gpus[0][0],{'device_id': gpus[0][1]})
