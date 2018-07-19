import os
import numpy as np
import copy
from enum import Enum

from constant import *


class Method(Enum):
    FFD = 1
    Knapsack = 2
    Analyse = 3
    If_Search_Has_Init_Conflict = 4


class Instance(object):
    def __init__(self, id, app_id, machine_id):
        self.id = id
        self.app_id = app_id
        self.machine_id = machine_id
        self.raw_machine_id = machine_id
        self.placed = False
        self.app = None
        self.machine = None
        self.done = False

        self._score = -1

    @staticmethod
    def from_csv_line(line):
        return Instance(*line.strip().split(","))

    @property
    def score(self):
        if self._score > 0:
            return self._score

        cpu = self.app.cpu / MAX_CPU_REQUEST
        mem = self.app.mem / MAX_MEM_REQUEST

        score = np.linalg.norm(cpu, ord=1) / LINE_SIZE * CPU_WEIGHT + np.linalg.norm(mem,
                                                                                     ord=1) / LINE_SIZE * MEM_WEIGHT
        self._score = score

        return score

    def __str__(self):
        return "Instance id(%s) app_id(%s) machine_id(%s) score(%f)" % (
            self.id, self.app_id, self.machine_id, self.score)


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
        self.interfer_others = {}
        self.interfer_by_others = {}

    @staticmethod
    def from_csv_line(line):
        return Application(*line.strip().split(","))

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
        self.pmp = np.array([0] * 3)
        self.pmp_cap = np.array([0] * 3)
        self.app_interfers = {}

        self.cpu = np.full(int(LINE_SIZE), self.cpu_capacity)
        self.mem = np.full(int(LINE_SIZE), self.mem_capacity)
        self.cpu_use = np.zeros(int(LINE_SIZE))
        self.mem_use = np.zeros(int(LINE_SIZE))
        self.disk_use = 0
        self.insts = {}
        self.apps_id = []
        self.bins = []
        self.app_inst = {}

        self.p_num = 0
        self.m_num = 0
        self.pm_num = 0

    def put_inst(self, inst):
        self.insts[inst.id] = inst
        self.cpu_use += inst.app.cpu
        self.mem_use += inst.app.mem
        self.disk_use += inst.app.disk
        self.p_num += inst.app.p
        self.m_num += inst.app.m
        self.pm_num += inst.app.pm

        self.apps_id.append(inst.app.id)

    def remove_inst(self, inst):
        self.insts.pop(inst.id)
        self.cpu_use -= inst.app.cpu
        self.mem_use -= inst.app.mem
        self.disk_use -= inst.app.disk
        self.p_num -= inst.app.p
        self.m_num -= inst.app.m
        self.pm_num -= inst.app.pm

        self.apps_id.remove(inst.app.id)

    def take_out(self, inst):
        del(self.insts[inst.id])

        self.cpu_use -= inst.app.cpu
        self.mem_use -= inst.app.mem
        self.disk_use -= inst.app.disk
        self.p_num -= inst.app.p
        self.m_num -= inst.app.m
        self.pm_num -= inst.app.pm

        self.apps_id.remove(inst.app.id)

    def can_deploy_inst(self, inst):
        app = inst.app
        if self.disk_capacity - self.disk_use <= app.disk or (self.cpu / 2 - self.cpu_use <= app.cpu).any() or \
                (self.mem - self.mem_use <= app.mem).any() or self.p_capacity - self.p_num <= app.p or \
                self.m_capacity - self.m_num <= app.m or self.pm_capacity - self.pm_num <= app.pm:
            return False
        # this machine can hold the instance in memory view
        for app_a in app.interfer_by_others.values():
            if self.apps_id.count(app_a.id) > 0:  # already deployed app_a on machine
                if app_a.interfer_others.has_key(app.id):
                    if app_a.interfer_others[app.id] <= self.apps_id.count(app.id):
                        return False
        for app_b in app.interfer_others.keys():
            if self.apps_id.count(app_b) > 0:  # already deployed app_b on self
                if app.interfer_others[app_b] < self.apps_id.count(app_b):
                    return False
        return True

    def can_put_inst(self, inst):
        if self.disk_use + inst.app.disk > self.disk_capacity:
            return False
        if any((self.cpu_use + inst.app.cpu) > self.cpu_capacity / 2):
            return False
        if any((self.mem_use + inst.app.mem) > self.mem_capacity):
            return False
        if any((self.pmp + np.array([inst.app.p, inst.app.m, inst.app.pm])) > self.pmp_cap):
            return False

        app_dic = copy.deepcopy(self.app_count)
        has = app_dic[inst.app.id] if inst.app.id in app_dic else 0
        for app, cnt in app_dic.iteritems():
            # print app, inst.app.id
            if (app, inst.app.id) in self.app_interfers:
                if has + 1 > self.app_interfers[(app, inst.app.id)].num:
                    return False
        for app, cnt in app_dic.iteritems():
            if (inst.app.id, app) in self.app_interfers:
                if cnt > self.app_interfers[(inst.app.id, app)].num:
                    return False
        # for app1, cnt2 in app_dic.iteritems():
        #     for app2, cnt2 in app_dic.iteritems():
        #         if (app1, app2) in self.app_interfers:
        #             if cnt2 > self.app_interfers[(app1, app2)].num:
        #                 return False
        return True

    @property
    def cpu_score(self):
        return max(self.cpu_use / self.cpu_capacity)

    @property
    def mem_score(self):
        return max(self.mem_use / self.mem_capacity)

    @property
    def app_count(self):
        dic = {}
        for inst in self.insts.values():
            dic[inst.app.id] = dic[inst.app.id] + 1 if inst.app.id in dic else 1
        return dic

    @property
    def inter_inst_num(self):
        app_dict = {}

        for inst in self.insts.values():
            app_dict[inst.app_id] = app_dict[inst.app_id] + 1 if inst.app_id in app_dict else 1
        interfer_cnt = 0
        for app_a in app_dict.keys():
            for app_b in app_dict.keys():
                if (app_a, app_b) in self.app_interfers:
                    if app_dict[app_b] > self.app_interfers[(app_a, app_b)].num:
                        interfer_cnt += app_dict[app_b]
        return interfer_cnt

    @property
    def violate_apps(self):
        app_dict = {}
        for inst in self.insts.values():
            app_dict[inst.app_id] = app_dict[inst.app_id] + 1 if inst.app_id in app_dict else 1
        apps = set()
        for app_a in app_dict.keys():
            for app_b in app_dict.keys():
                if (app_a, app_b) in self.app_interfers:
                    if app_dict[app_b] > self.app_interfers[(app_a, app_b)].num:
                        apps.add(app_b)
        return apps

    @staticmethod
    def from_csv_line(line):
        return Machine(*line.strip().split(","))

    def __str__(self):
        return "Machine id(%s) disk(%d/%d) cpu_score(%f) mem_score(%f) bins(%s)" % (
            self.id, self.disk_use, self.disk_capacity, self.cpu_score, self.mem_score,
            ",".join([str(i.app.disk) for i in self.insts.values()]))


class AppInterference(object):
    def __init__(self, app_a, app_b, num):
        self.app_a = app_a
        self.app_b = app_b
        self.num = int(num)
        if app_a == app_b:
            self.num += 1

    @staticmethod
    def from_csv_line(line):
        return AppInterference(*line.split(","))

    def __str__(self):
        return "App_a (%s) App_b (%s) number (%d)" % (
            self.app_a, self.app_b, self.num
        )


def read_from_csv(directory_path):
    instances = []
    instance_index = {}
    for line in open(os.path.join(directory_path, INSTANCE_INPUT_FILE)):
        instance = Instance.from_csv_line(line)
        instance_index[instance.id] = len(instances)
        instances.append(instance)

    machines = []
    machine_index = {}
    for line in open(os.path.join(directory_path, MACHINE_INPUT_FILE)):
        machine = Machine.from_csv_line(line)
        machine_index[machine.id] = len(machines)
        machines.append(machine)

    apps = []
    app_index = {}
    for line in open(os.path.join(directory_path, APP_INPUT_FILE)):
        app = Application.from_csv_line(line)
        app_index[app.id] = len(apps)
        apps.append(app)

    app_interfer = []
    for line in open(os.path.join(directory_path, APP_INTERFER_FILE)):
        app_interfer.append(AppInterference.from_csv_line(line))

    return instances, apps, machines, app_interfer, app_index, machine_index, instance_index


def get_apps_instances(insts, apps, app_index):
    for inst in insts:
        index = app_index[inst.app_id]
        apps[index].instances.append(inst)
        inst.app = apps[index]


def prepare_apps_interfers(app_interfers, app_index, apps):
    for app_interfer in app_interfers:
        index_a = app_index[app_interfer.app_a]
        index_b = app_index[app_interfer.app_b]
        apps[index_a].interfer_others[app_interfer.app_b] = app_interfer.num
        apps[index_b].interfer_by_others[app_interfer.app_a] = apps[index_a]
