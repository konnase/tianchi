MACHINE_INPUT_FILE = "scheduling_preliminary_b_machine_resources_20180726.csv"
APP_INPUT_FILE = "scheduling_preliminary_b_app_resources_20180726.csv"
INSTANCE_INPUT_FILE = "scheduling_preliminary_b_instance_deploy_20180726.csv"
APP_INTERFER_FILE = "scheduling_preliminary_b_app_interference_20180726.csv"

LINE_SIZE = 98

MAX_CPU_REQUEST = 32
MAX_MEM_REQUEST = 128

CPU_WEIGHT = 0.2
MEM_WEIGHT = 0.8

LARGE_CPU_INST = 15
LARGE_MEM_INST = 24
LARGE_DISK_INST = 300

FLOAT_EPS = 0.000001

DISK_CAP_LARGE = 2457
DISK_CAP_SMALL = 1440


class global_var(object):
    CPU_UTIL_LARGE = 1
    CPU_UTIL_SMALL = 1


@property
def cpu_util_large():
    return global_var.CPU_UTIL_LARGE


def set_cpu_util_large(cpu_util_large):
    global_var.CPU_UTIL_LARGE = cpu_util_large


@property
def cpu_util_small():
    return global_var.CPU_UTIL_SMALL


def set_cpu_util_small(cpu_util_small):
    global_var.CPU_UTIL_SMALL = cpu_util_small
