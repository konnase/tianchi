# coding=utf-8
import os
import numpy as np
import scheduler.config as cfg

TS_COUNT = cfg.TS_COUNT
RESOURCE_LEN = cfg.RESOURCE_LEN


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
        self.exchanged = 0

        # self._metric = -1

    @staticmethod
    def from_csv_line(line):
        # 这里只设置 inst_id
        return Instance(line.strip().split(",")[0])

    @staticmethod
    def get_undeployed_insts(instances):
        result = []
        for i in instances:
            if not i.deployed:
                result.append(i)
        return result

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
        self.pmp_cap = np.array([float(p_cap), float(m_cap), float(pm_cap)])

        if self.is_large_machine:
            self.CPU_UTIL_THRESHOLD = cfg.CPU_UTIL_LARGE
        else:
            self.CPU_UTIL_THRESHOLD = cfg.CPU_UTIL_SMALL

        # 修改 capacity 中cpu的比例，
        # 但不修改 self.cpu_cap，后者用于计算 cpu_util 和 score
        self.capacity = np.hstack((np.full(int(TS_COUNT), self.cpu_cap * self.CPU_UTIL_THRESHOLD),
                                   np.full(int(TS_COUNT), self.mem_cap),
                                   np.array([self.disk_cap]),
                                   self.pmp_cap))

        self.full_cap = np.hstack((np.full(int(TS_COUNT), self.cpu_cap),
                                   np.full(int(TS_COUNT), self.mem_cap),
                                   np.array([self.disk_cap]),
                                   self.pmp_cap))

        self.usage = np.zeros(RESOURCE_LEN)

        self.inst_kv = {}  # <inst_id, instance>
        self.app_cnt_kv = {}  # <app_id, cnt>
        self.app_inst_kv = {}  # <app_id,only_one_inst> 用于减少重复搜索同类应用实例

    # 根据某维资源（这里是disk）检查，需事先设置了该维资源值
    @property
    def is_large_machine(self):
        return self.disk_cap == cfg.DISK_CAP_LARGE

    @property
    def cpu_usage(self):
        return self.usage[0:TS_COUNT]

    @property
    def is_cpu_overload(self):
        return np.any(np.around(self.cpu_usage, 8) > self.cpu_cap)

    @property
    def mem_usage(self):
        return self.usage[TS_COUNT:TS_COUNT * 2]

    @property
    def is_mem_overload(self):

        return np.any(np.around(self.mem_usage, 8) > self.mem_cap)

    @property
    def disk_usage(self):
        return self.usage[TS_COUNT * 2]

    @property
    def is_disk_overload(self):
        return self.disk_usage > self.disk_cap

    @property
    def pmp_usage(self):
        return self.usage[TS_COUNT * 2 + 1:RESOURCE_LEN]

    @property
    def pmp_overload_cnt(self):
        return np.sum((self.pmp_usage > self.pmp_cap))

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

        return cpu_score(self.cpu_usage,self.cpu_cap, len(self.inst_kv))

    # 将实例添加到机器上，不检查资源和亲和约束
    def put_inst(self, inst):
        # 幂等性，若inst已经部署到这台机器了，则直接跳过
        # 这个放底层判断影响效率
        if inst.id in self.inst_kv.keys():
            return

        self.usage += inst.app.resource

        # 这个放底层判断影响效率
        # if inst.machine is not None:
        #     inst.machine.remove_inst(inst)

        inst.machine = self
        inst.deployed = True
        self.inst_kv[inst.id] = inst

        app_id = inst.app.id
        self.app_cnt_kv.setdefault(app_id, 0)
        self.app_cnt_kv[app_id] += 1

        self.app_inst_kv.setdefault(app_id, inst)

    def remove_inst(self, inst):
        if not self.inst_kv.has_key(inst.id):
            return

        self.usage -= inst.app.resource
        inst.machine = None
        inst.deployed = False
        self.inst_kv.pop(inst.id)
        app_id = inst.app.id
        self.app_cnt_kv[app_id] -= 1

        # 必须移除实例个数为0的应用，否则检查亲和约束会干扰循环
        if self.app_cnt_kv[app_id] == 0:
            self.app_cnt_kv.pop(app_id)
            self.app_inst_kv.pop(app_id)

    def clear_instances(self):
        # 注意：循环中修改列表
        for inst in self.inst_kv.values()[:]:
            self.remove_inst(inst)

        self.usage = np.zeros(RESOURCE_LEN)  # 防止舍入误差？

    def can_put_inst(self, inst, full_cap=False):
        # if self.inst_kv.has_key(inst.id):
        #     return True
        # else:
        return not (self.out_of_capacity_inst(inst, full_cap) or self.has_conflict_inst(inst))

    # capacity 中 cpu 部分已经乘以了CPU_UTIL系数
    def out_of_capacity_inst(self, inst, full_cap=False):
        if full_cap:
            return np.any(np.around(self.usage + inst.app.resource, 8) > self.full_cap)
        else:
            return np.any(np.around(self.usage + inst.app.resource, 8) > self.capacity)

    # 这里与没有乘以CPU_UTIL系数的资源量比较
    def out_of_full_capacity(self):
        return np.any(np.around(self.usage, 8) > self.full_cap)

    # 亲和性冲突
    def has_conflict_inst(self, inst):
        appId_b = inst.app.id
        appCnt_b = self.app_cnt_kv.get(appId_b, 0)

        for appId_a, appCnt_a in self.app_cnt_kv.items():
            if appCnt_b + 1 > AppInterference.limit_of(appId_a, appId_b):
                return True

            if appCnt_a > AppInterference.limit_of(appId_b, appId_a):
                return True

        return False

    # [(appId_a, appId_b, appCnt_b, limit)]
    def get_conflict_list(self):
        result = []
        for appId_a in self.app_cnt_kv.keys():
            for appId_b, appCnt_b in self.app_cnt_kv.iteritems():
                limit = AppInterference.limit_of(appId_a, appId_b)
                if appCnt_b > limit:
                    result.append((appId_a, appId_b, appCnt_b, limit))

        return result

    def has_conflict(self):
        for appId_a in self.app_cnt_kv.keys():
            for appId_b, appCnt_b in self.app_cnt_kv.iteritems():
                limit = AppInterference.limit_of(appId_a, appId_b)
                if appCnt_b > limit:
                    return True

        return False

    @staticmethod
    # 合计一组机器的成本分数
    def total_score(machines):
        s = 0
        for m in machines:
            s += m.score
        return s

    @staticmethod
    def used_machine_count(machines):
        cnt = 0
        for m in machines:
            if m.disk_usage > 0:
                cnt += 1
        return cnt

    # 分别获取资源超额和违反亲和约束的列表，
    @staticmethod
    def get_abnormal_machines(machines):
        machines.sort(key=lambda x: x.disk_cap, reverse=True)
        out_of_cap_set = []
        conflict_set = []
        for m in machines:
            if m.out_of_full_capacity():
                out_of_cap_set.append(m)
            if m.has_conflict():
                conflict_set.append(m)
        return out_of_cap_set, conflict_set

    # 清空一组机器上部署的实例
    @staticmethod
    def empty(machines):
        for m in machines:
            m.clear_instances()

    @staticmethod
    def from_csv_line(line):
        return Machine(*line.strip().split(","))

    def __str__(self):
        return "Machine id(%s) score(%f) disk(%d/%d) cpu_usage(%f) mem_usage(%f) bins(%s)" % (
            self.id, self.score, self.disk_usage, self.disk_cap, self.cpu_util_max, self.mem_util_max,
            ",".join([str(i.app.disk) for i in self.inst_kv.values()]))

    def to_search_str(self):
        return "total(%f,%.2f,%.2f,%.2f,%d): {%s} (%s)\n" % (
            self.score, self.cpu_util_max, self.cpu_util_avg, self.mem_util_max, self.disk_usage,
            ",".join([str(int(i.app.disk)) for i in self.inst_kv.values()]),
            ",".join([i.id for i in self.inst_kv.values()]))


def cpu_score(cpu_usage, cpu_cap, inst_count):
    x = np.maximum(cpu_usage / cpu_cap - 0.5, 0)  # max(c - beta, 0)
    # return np.average(1 + 10 * (np.exp(x) - 1))
    return np.average(1 + (1 + inst_count) * (np.exp(x) - 1))


def write_to_search(path, machines):
    with open(path, "w") as f:
        print("writing to %s" % path)
        for m in machines:
            if m.disk_usage == 0:
                continue
            f.write(m.to_search_str())


# [(inst_id, machine_id)]
def write_to_submit_csv(path, submit_result):
    print("writing to %s" % (path))
    with open(path, "w") as f:
        for rounds, inst_id, machine_id in submit_result:
            f.write("{0},{1},{2}\n".format(rounds, inst_id, machine_id))


def read_from_csv(project_path):
    machines = []
    machine_kv = {}
    for line in open(os.path.join(project_path, cfg.MACHINE_INPUT_FILE)):
        m = Machine.from_csv_line(line)
        machine_kv[m.id] = m
        machines.append(m)

    apps = []
    app_kv = {}
    for line in open(os.path.join(project_path, cfg.APP_INPUT_FILE)):
        app = Application.from_csv_line(line)
        app_kv[app.id] = app
        apps.append(app)

    for line in open(os.path.join(project_path, cfg.APP_INTERFER_FILE)):
        AppInterference.append(line)

    instances = []
    instance_kv = {}
    for line in open(os.path.join(project_path, cfg.INSTANCE_INPUT_FILE)):
        line = line.rstrip('\n')
        inst = Instance.from_csv_line(line)
        parts = line.split(',')  # inst_id,app_id,machine_id
        inst.app = app_kv[parts[1]]
        inst.app.instances.append(inst)

        # machine_id不为空，
        # 则读入初始部署，且不考虑资源和亲和约束
        if parts[2] != '':
            m = machine_kv[parts[2]]
            can_deploy = m.can_put_inst(inst, full_cap=True)
            m.put_inst(inst)  # 部署inst后，会改变机器状态，故需事先保存标志
            inst.deployed = can_deploy

        instance_kv[inst.id] = inst  # 字典直接保存实例对象（的引用）
        instances.append(inst)

    return instances, apps, machines, instance_kv, app_kv, machine_kv
