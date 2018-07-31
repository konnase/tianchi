# coding=utf-8
import math

import numpy as np

from config import *
from scheduler.models import Machine

# todo: 重构过的ffd执行结果比之前版本变差很多
class FFD(object):
    def __init__(self, instances, apps, machines):
        self.instances = instances
        self.apps = apps
        self.machines = machines

        self.apps.sort(key=lambda x: x.disk, reverse=True)
        self.machines.sort(key=lambda x: x.disk_cap, reverse=True)

        self.submit_result = []

        # print machines[0].CPU_UTIL, machines[3001].CPU_UTIL

    def first_fit(self, inst, machines, large_machine=False):
        for m in machines:
            if large_machine and not m.is_large_machine:
                continue
            if m.can_put_inst(inst):
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
                self.first_fit(inst, self.machines)

    def fit_large_inst(self, machines):
        print "starting fit_large_inst"
        for app in self.apps:
            if app.disk > LARGE_DISK_INST \
                    or np.any(app.cpu > LARGE_CPU_INST) \
                    or np.any(app.mem > LARGE_MEM_INST):
                for inst in app.instances:
                    self.first_fit(inst, machines, large_machine=True)

    def fit(self):
        # print Machine.total_score(self.machines)
        self.resolve_init_conflict(self.machines)
        self.fit_large_inst(self.machines)
        print "starting fit"
        for app in self.apps:
            for inst in app.instances:
                if not inst.deployed:
                    self.first_fit(inst, self.machines)

    def write_to_csv(self):
        total_score = Machine.total_score(self.machines)
        s = ("%.2f" % total_score).replace(".", "_")
        csv = "submit%s.csv" % s
        print "writing to", csv
        with open(csv, "w") as f:
            for inst_id, machine_id in self.submit_result:
                f.write("{0},{1}\n".format(inst_id, machine_id))

        machine_cnt = len(filter(lambda x: x.disk_usage > 0, self.machines))

        path = "search-result/search_%d_%dm" % (int(math.ceil(total_score)), machine_cnt)
        with open(path, "w") as f:
            print "writing to %s" % path
            for machine in self.machines:
                if machine.disk_usage == 0:
                    continue
                f.write(machine.to_search_str())
