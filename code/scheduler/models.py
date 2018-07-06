import os
import numpy as np
from constant import *


class Instance(object):
    def __init__(self, id, app_id, machine_id):
        self.id = id
        self.app_id = app_id
        self.machine_id = machine_id

    @staticmethod
    def from_csv_line(line):
        return Instance(*line.split(","))

    def __str__(self):
        return "Instance id(%s) app_id(%s) machine_id(%s)" % (self.id, self.app_id, self.machine_id)


class Application(object):
    def __init__(self, id, cpu_line, mem_line, disk, p, m, pm):
        self.id = id
        self.cpu = np.array([float(i) for i in cpu_line.split("|")])
        self.mem = np.array([float(i) for i in mem_line.split("|")])
        self.disk = float(disk)
        self.p = int(p)
        self.m = int(m)
        self.pm = int(pm)
        self.instances = []

    @staticmethod
    def from_csv_line(line):
        return Application(*line.split(","))

    def __str__(self):
        return "App id(%s) cpu(%s) mem(%s) disk(%d) p(%d) m(%d) pm(%d)" % (
            self.id, self.cpu, self.mem, self.disk, self.p, self.m, self.pm)


class Machine(object):
    def __init__(self, id, cpu_capacity, mem_capacity, disk_capacity, p_capacity, m_capacity, pm_capacity):
        self.id = id
        self.cpu_capacity = float(cpu_capacity)
        self.mem_capacity = float(mem_capacity)
        self.disk_capacity = float(disk_capacity)
        self.p_capacity = int(p_capacity)
        self.m_capacity = int(m_capacity)
        self.pm_capacity = int(pm_capacity)

        self.cpu = np.array([cpu_capacity * LINE_SIZE])
        self.mem = np.array([mem_capacity * LINE_SIZE])
        self.disk = np.array([disk_capacity * LINE_SIZE])
        self.cpu_use = 0
        self.mem_use = 0
        self.disk_use = 0
        self.apps = []

        self.p_num = 0
        self.m_num = 0
        self.pm_num = 0

    @staticmethod
    def from_csv_line(line):
        return Machine(*line.split(","))

    def __str__(self):
        return "Machine id(%s) cpu(%d) mem(%d) disk(%d) p(%d) m(%d) pm(%d)" % (
            self.id, self.cpu_capacity, self.mem_capacity, self.disk_capacity, self.p_capacity, self.m_capacity,
            self.pm_capacity)


class AppInterference(object):
    def __init__(self, app_a, app_b, num):
        self.app_a = app_a
        self.app_b = app_b
        self.num = int(num)

    @staticmethod
    def from_csv_line(line):
        return AppInterference(*line.split(","))

    def __str__(self):
        return "App_a (%s) App_b (%s) number (%d)" % (
            self.app_a, self.app_b, self.num
        )


def read_from_csv(directory_path):
    instances = []
    for line in open(os.path.join(directory_path, INSTANCE_INPUT_FILE)):
        instances.append(Instance.from_csv_line(line))

    machines = []
    for line in open(os.path.join(directory_path, MACHINE_INPUT_FILE)):
        machines.append(Machine.from_csv_line(line))

    applications = []
    for line in open(os.path.join(directory_path, APP_INPUT_FILE)):
        applications.append(Application.from_csv_line(line))

    app_interfer = []
    for line in open(os.path.join(directory_path, APP_INTERFER_FILE)):
        app_interfer.append(AppInterference.from_csv_line(line))

    return instances, applications, machines, app_interfer


def get_apps_instances(instances, applications):
    for inst in instances:
        for app in applications:
            if inst.app_id == app.id:
                app.instances.append(inst)
                break