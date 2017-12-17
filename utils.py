""" This module contains utilitary functions."""

from pynvml import *

def format_name(strf):
    return strf.lower().replace(" ","_")

def get_device_name(idx):
    nvmlInit()
    handle = nvmlDeviceGetHandleByIndex(idx)
    name = nvmlDeviceGetName(handle)
    nvmlShutdown()
    return name