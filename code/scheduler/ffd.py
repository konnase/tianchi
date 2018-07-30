# coding=utf-8
import math

import numpy as np

from config import *
from scheduler.models import Machine


class FFD(object):
    def __init__(self, instances, apps, machines):
        self.instances = instances
        self.apps = apps
        self.machines = machines

        self.apps.sort(key=lambda x: x.disk, reverse=True)
        self.machines.sort(key=lambda x: x.disk_cap, reverse=True)

        self.submit_result = []

    def first_fit(self, inst, machines):
        for m in machines:
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
            if app.disk < LARGE_DISK_INST \
                    and np.all(app.cpu < LARGE_CPU_INST) \
                    and np.all(app.mem < LARGE_MEM_INST):
                continue

            for inst in app.instances:
                if not inst.deployed:
                    self.first_fit(inst, machines)

    def fit(self):
        self.resolve_init_conflict(self.machines)
        self.fit_large_inst(self.machines)
        print "starting fit"
        for app in self.apps:
            for inst in app.instances:
                if not inst.deployed:
                    self.first_fit(inst, self.machines)

    def write_to_csv(self):
        with open("submit.csv", "w") as f:
            print "writing to submit.csv"
            for inst_id, machine_id in self.submit_result:
                f.write("{0},{1}\n".format(inst_id, machine_id))

        total_score = Machine.total_score(self.machines)
        machine_cnt = len(filter(lambda x: x.disk_usage > 0, self.machines))

        path = "search-result/search_%d_%dm" % (int(math.ceil(total_score)), machine_cnt)
        with open(path, "w") as f:
            print "writing to %s" % path
            for machine in self.machines:
                if machine.disk_usage == 0:
                    continue
                f.write(machine.to_search_str())
