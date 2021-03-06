# MACHINE_INPUT_FILE = "scheduling_preliminary_b_machine_resources_20180726.csv"
# APP_INPUT_FILE = "scheduling_preliminary_b_app_resources_20180726.csv"
# INSTANCE_INPUT_FILE = "scheduling_preliminary_b_instance_deploy_20180726.csv"
# APP_INTERFER_FILE = "scheduling_preliminary_b_app_interference_20180726.csv"
MACHINE_INPUT_FILE = "machine_resources.a.csv"
APP_INPUT_FILE = "app_resources.csv"
INSTANCE_INPUT_FILE = "instance_deploy.a.csv"
APP_INTERFER_FILE = "app_interference.csv"

TS_COUNT = 98
RESOURCE_LEN = 200

CPU_UTIL_LARGE = 0.682
CPU_UTIL_SMALL = 0.6

DISK_CAP_LARGE = 2457
DISK_CAP_SMALL = 1440

# MAX_CPU_REQUEST = 64  # todo: check this
# MAX_MEM_REQUEST = 288

# CPU_WEIGHT = 0.2
# MEM_WEIGHT = 0.8

LARGE_CPU_INST = 15  # todo: check this for data set b
LARGE_MEM_INST = 24
LARGE_DISK_INST = 300
