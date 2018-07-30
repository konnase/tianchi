# coding=utf-8
import os
import numpy as np
from config import *


class Instance(object):
    def __init__(self, id, app=None, machine=None):
        self.id = id
        self.app = app
        self.machine = machine

        # 实例是否部署了
        # 当实例还没有部署到机器（machine == None），
        # 或者已经部署到机器（machine == a），但需要迁移，还没有确定目标机器时，
        # 设置此标志为False
        self.deployed = False

        # self._metric = -1

    @staticmethod
    def from_csv_line(line):
        # 这里只设置 inst_id
        return Instance(line.strip().split(",")[0])

    # @property
    # # 自定义的指标
    # def metric(self):
    #     if self._metric > 0:
    #         return self._metric
    #
    #     cpu = self.app.cpu / MAX_CPU_REQUEST
    #     mem = self.app.mem / MAX_MEM_REQUEST
    #
    #     # linalg.norm(x, ord=1) 一阶范数是绝对值之和
    #     _m = np.linalg.norm(cpu, ord=1) * CPU_WEIGHT + np.linalg.norm(mem, ord=1) * MEM_WEIGHT
    #     self._metric = _m / TS_COUNT
    #
    #     return self._metric

    def __str__(self):
        return "Instance_id(%s) app_id(%s) machine_id(%s)" % (
            self.id, self.app.id, self.machine.id)


class Application(object):
    def __init__(self, id, cpu_line, mem_line, disk, p, m, pm):
        self.id = id
        self.cpu = np.array([float(i) for i in cpu_line.split("|")])
        self.mem = np.array([float(i) for i in mem_line.split("|")])
        self.disk = float(disk)
        self.instances = []

        # 参考官方评分代码，把所有资源保存为一个向量，当然，要对cpu util特别处理
        self.resource = np.hstack((self.cpu,
                                   self.mem,
                                   np.array([self.disk, float(p), float(m), float(pm)])))

    @staticmethod
    def from_csv_line(line):
        return Application(*line.strip().split(","))

    def __str__(self):
        return "App id(%s) resource(%s)" % (self.id, self.resource)


class AppInterference(object):
    kv = {}  # <(appId_a, appId_b), limit>

    @staticmethod
    def append(line):
        parts = line.split(",")

        appId_a = parts[0]
        appId_b = parts[1]
        limit = int(parts[2])
        if appId_a == appId_b:
            limit += 1
        # 没有重复的规则
        AppInterference.kv[(appId_a, appId_b)] = limit

    @staticmethod
    def limit_of(appId_a, appId_b):
        # 如果没有对应规则，返回一个很大的数
        return AppInterference.kv.get((appId_a, appId_b), 1e9)


class Machine(object):
    def __init__(self, id, cpu_cap, mem_cap, disk_cap, p_cap, m_cap, pm_cap):
        self.id = id
        self.cpu_cap = float(cpu_cap)
        self.mem_cap = float(mem_cap)
        self.disk_cap = float(disk_cap)

        if self.is_large_machine:
            self.CPU_UTIL = CPU_UTIL_LARGE
        else:
            self.CPU_UTIL = CPU_UTIL_SMALL

        # 修改 capacity 中cpu的比例，
        # 但不修改 self.cpu_cap，后者用于计算 cpu_util 和 score
        self.capacity = np.hstack((np.full(int(TS_COUNT), self.cpu_cap * self.CPU_UTIL),
                                   np.full(int(TS_COUNT), self.mem_cap),
                                   np.array([self.disk_cap, float(p_cap), float(m_cap), float(pm_cap)])))

        self.full_cap = np.hstack((np.full(int(TS_COUNT), self.cpu_cap),
                                   np.full(int(TS_COUNT), self.mem_cap),
                                   np.array([self.disk_cap, float(p_cap), float(m_cap), float(pm_cap)])))

        self.usage = np.zeros(len(self.capacity))

        self.insts = {}  # <inst_id, instance>

        self.app_cnt_kv = {}  # <app_id, cnt>

    # 根据某维资源（这里是disk）检查，需事先设置了该维资源值
    @property
    def is_large_machine(self):
        return self.disk_cap == DISK_CAP_LARGE

    @property
    def cpu_usage(self):
        return self.usage[0:TS_COUNT]

    @property
    def mem_usage(self):
        return self.usage[TS_COUNT:TS_COUNT * 2]

    @property
    def disk_usage(self):
        return self.usage[TS_COUNT * 2]

    @property
    def pmp_usage(self):
        return self.usage[TS_COUNT * 2 + 1:RESOURCE_LEN]

    @property
    def cpu_util_max(self):
        return np.max(self.cpu_usage / self.cpu_cap)

    @property
    def cpu_util_avg(self):
        return np.average(self.cpu_usage / self.cpu_cap)

    @property
    def mem_util_max(self):
        return np.max(self.mem_usage / self.mem_cap)

    @property
    def mem_util_avg(self):
        return np.average(self.mem_usage / self.mem_cap)

    @property
    def score(self):
        if self.disk_usage == 0:
            return 0

        x = np.maximum(self.cpu_usage / self.cpu_cap - 0.5, 0)  # max(c - beta, 0)

        return np.average(1 + 10 * (np.exp(x) - 1))

    # 将实例添加到机器上，不检查资源和亲和约束
    def put_inst(self, inst):
        # 幂等性，若inst已经部署到这台机器了，则直接跳过
        if self.insts.has_key(inst.id):
            return

        self.usage += inst.app.resource

        if inst.machine is not None:
            inst.machine.remove_inst(inst)

        inst.machine = self
        inst.deployed = True
        self.insts[inst.id] = inst
        self.app_cnt_kv.setdefault(inst.app.id, 0)
        self.app_cnt_kv[inst.app.id] += 1

    def remove_inst(self, inst):
        if not self.insts.has_key(inst.id):
            return

        self.usage -= inst.app.resource
        inst.machine = None
        inst.deployed = False
        self.insts.pop(inst.id)
        self.app_cnt_kv[inst.app.id] -= 1

        # 必须移除实例个数为0的应用，否则检查亲和约束会干扰循环
        if self.app_cnt_kv[inst.app.id] == 0:
            self.app_cnt_kv.pop(inst.app.id)

    def clear_instances(self):
        # 注意：循环中修改列表
        for inst in self.insts.values()[:]:
            self.remove_inst(inst)

    def can_put_inst(self, inst):
        # if self.insts.has_key(inst.id):
        #     return True
        # else:
        return not (self.out_of_capacity(inst) or self.has_conflict(inst))

    # capacity 中 cpu 部分已经乘以了CPU_UTIL系数
    def out_of_capacity(self, inst):
        return any((self.usage + inst.app.resource) > self.capacity)

    # 这里与没有乘以CPU_UTIL系数的资源量比较
    def out_of_full_capacity(self):
        return any(self.usage > self.full_cap)

    # 亲和性冲突
    def has_conflict(self, inst):
        appId_b = inst.app.id
        appCnt_b = self.app_cnt_kv.get(appId_b, 0)

        for appId_a, appCnt_a in self.app_cnt_kv.iteritems():
            if appCnt_b + 1 > AppInterference.limit_of(appId_a, appId_b):
                return True

            if appCnt_a > AppInterference.limit_of(appId_b, appId_a):
                return True

        return False

    def get_conflict_list(self):
        result = []
        for appId_a in self.app_cnt_kv.keys():
            for appId_b, appCnt_b in self.app_cnt_kv.iteritems():
                limit = AppInterference.limit_of(appId_a, appId_b)
                if appCnt_b > limit:
                    result.append((appId_a, appId_b, appCnt_b, limit))

        return result

    @staticmethod
    # 合计一组机器的成本分数
    def total_score(machines):
        s = 0
        for m in machines:
            s += m.score
        return s

    # 清空一组机器上部署的实例
    @staticmethod
    def clear(machines):
        for m in machines:
            m.clear_instances()

    @staticmethod
    def from_csv_line(line):
        return Machine(*line.strip().split(","))

    def __str__(self):
        return "Machine id(%s) score(%f) disk(%d/%d) cpu_usage(%f) mem_usage(%f) bins(%s)" % (
            self.id, self.score, self.disk_usage, self.disk_cap, self.cpu_util_max, self.mem_util_max,
            ",".join([str(i.app.disk) for i in self.insts.values()]))


def read_from_csv(project_path):
    machines = []
    machine_kv = {}
    for line in open(os.path.join(project_path, MACHINE_INPUT_FILE)):
        m = Machine.from_csv_line(line)
        machine_kv[m.id] = m
        machines.append(m)

    apps = []
    app_kv = {}
    for line in open(os.path.join(project_path, APP_INPUT_FILE)):
        app = Application.from_csv_line(line)
        app_kv[app.id] = app
        apps.append(app)

    for line in open(os.path.join(project_path, APP_INTERFER_FILE)):
        AppInterference.append(line)

    instances = []
    instance_kv = {}
    for line in open(os.path.join(project_path, INSTANCE_INPUT_FILE)):
        line = line.rstrip('\n')
        inst = Instance.from_csv_line(line)
        parts = line.split(',')  # inst_id,app_id,machine_id
        inst.app = app_kv[parts[1]]
        inst.app.instances.append(inst)

        # machine_id不为空，
        # 则读入初始部署，且不考虑资源和亲和约束
        if parts[2] != '':
            m = machine_kv[parts[2]]
            can_deploy = m.can_put_inst(inst)
            m.put_inst(inst)  # 部署inst后，会改变机器状态，故需事先保存标志
            inst.deployed = can_deploy

        instance_kv[inst.id] = inst  # 字典直接保存实例对象（的引用）
        instances.append(inst)

    return instances, apps, machines, instance_kv, app_kv, machine_kv
