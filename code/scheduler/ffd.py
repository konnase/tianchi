# coding=utf-8
import math

import numpy as np

import config as cfg

from scheduler.models import Machine


class FFD(object):
    def __init__(self, instances, apps, machines):
        self.instances = instances
        self.apps = apps
        self.machines = machines

        self.apps.sort(key=lambda x: x.disk, reverse=True)
        self.machines.sort(key=lambda x: x.disk_cap, reverse=True)

        self.submit_result = []

    def first_fit(self, inst, machines, large_machine=False):
        for m in machines:
            if large_machine and not m.is_large_machine:
                continue
            if m != inst.machine and m.can_put_inst(inst):  # 使用 CPU_UTIL_THRESHOLD
                m.put_inst(inst)
                self.submit_result.append((inst.id, m.id))
                break
        else:
            raise Exception("%s is not deployed" % inst.id)

    def resolve_init_conflict(self, machines):
        print "starting resolve_init_conflict"
        # 初始部署就存在冲突的实例
        for inst in self.instances:
            if not inst.deployed and inst.machine is not None:
                self.first_fit(inst, machines)

    # 初始部署就存在的cpu_util超高的机器
    def migrate_high_cpu_util(self, machines):
        print "starting migrate_high_cpu_util"
        for machine in machines:
            if machine.cpu_util_max <= machine.CPU_UTIL_THRESHOLD:
                continue
            for inst in machine.inst_kv.values():
                self.first_fit(inst, machines)
                if machine.cpu_util_max < machine.CPU_UTIL_THRESHOLD:
                    break  # cpu_util 降下来就不再迁移了

    def fit_large_inst(self, machines):
        print "starting fit_large_inst"
        for app in self.apps:
            if app.disk > cfg.LARGE_DISK_INST \
                    or np.any(app.cpu > cfg.LARGE_CPU_INST) \
                    or np.any(app.mem > cfg.LARGE_MEM_INST):
                for inst in app.instances:
                    if not inst.deployed:
                        self.first_fit(inst, machines, large_machine=True)

    def fit(self):
        print "using CPU_UTIL_LARGE: %.2f and CPU_UTIL_SMALL: %.2f" % (self.machines[0].CPU_UTIL_THRESHOLD,
                                                                       self.machines[3000].CPU_UTIL_THRESHOLD)
        self.resolve_init_conflict(self.machines)
        self.migrate_high_cpu_util(self.machines)
        self.fit_large_inst(self.machines)
        self.resolve_init_conflict(self.machines)
        print "starting fit"
        for app in self.apps:
            for inst in app.instances:
                if not inst.deployed:
                    self.first_fit(inst, self.machines)

    def write_to_csv(self):
        total_score = Machine.total_score(self.machines)
        s = ("%.2f" % total_score).replace(".", "_")
        csv = "submit_%s.csv" % s
        print "writing to", csv
        with open(csv, "w") as f:
            for inst_id, machine_id in self.submit_result:
                f.write("{0},{1}\n".format(inst_id, machine_id))

        machine_cnt = len(filter(lambda x: x.disk_usage > 0, self.machines))

        path = "search-result/search_%s_%dm" % (s, machine_cnt)
        with open(path, "w") as f:
            print "writing to %s" % path
            for machine in self.machines:
                if machine.disk_usage == 0:
                    continue
                f.write(machine.to_search_str())
