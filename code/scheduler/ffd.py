# coding=utf-8
import numpy as np

import scheduler.config as cfg

import time

from scheduler.models import Instance, Machine, write_to_submit_csv, write_to_search


class FFD(object):
    def __init__(self, instances, apps, machines, app_kv):
        self.instances = instances
        self.apps = apps
        self.machines = machines
        self.app_kv = app_kv
        self.undeployed_inst_cnt = 0
        self.pickFrom = {}

        self.apps.sort(key=lambda x: x.disk, reverse=True)
        self.machines.sort(key=lambda x: x.disk_cap, reverse=True)

        self.submit_result = []

    def first_fit(self, inst, machines, large_machine=False):
        for m in machines:
            if large_machine and not m.is_large_machine:
                continue
            # 使用 CPU_UTIL_THRESHOLD
            if m != inst.machine and m.can_put_inst(inst):
                if inst.exchanged == 1:
                    continue
                self.pickFrom[inst] = inst.machine
                inst.exchanged += 1
                m.put_inst(inst)
                print("deployed %s of %s on %s" % (inst.id, inst.app.id, m.id))
                self.submit_result.append((inst.exchanged, inst.id, m.id))
                break
        else:
            print("%s is not deployed" % inst.id)
            self.undeployed_inst_cnt += 1
            if self.undeployed_inst_cnt > 50:
                raise Exception("Abort! too many undeployed instances")

    def resolve_init_conflict(self, machines):
        print("starting resolve_init_conflict")
        time.sleep(2)
        # 初始部署就存在冲突的实例
        for inst in self.instances:
            if not inst.deployed and inst.machine is not None:
                self.first_fit(inst, machines)

    # 初始部署就存在的cpu_util超高的机器
    def migrate_high_cpu_util(self, machines):
        print("starting migrate_high_cpu_util")
        time.sleep(2)
        for machine in machines:
            if machine.cpu_util_max <= machine.CPU_UTIL_THRESHOLD:
                continue
            for inst in machine.inst_kv.values():
                self.first_fit(inst, machines)
                if machine.cpu_util_max < machine.CPU_UTIL_THRESHOLD:
                    break  # cpu_util 降下来就不再迁移了

    def fit_large_inst(self, machines):
        print("starting fit_large_inst")
        time.sleep(2)
        for app in self.apps:
            if app.disk > LARGE_DISK_INST \
                    or np.any(app.cpu > LARGE_CPU_INST) \
                    or np.any(app.mem > LARGE_MEM_INST):
                for inst in app.instances:
                    if not inst.deployed:
                        self.first_fit(inst, machines, large_machine=True)

    def fit_all(self, machines):
        print("starting fit_all")
        time.sleep(2)
        for app in self.apps:
            for inst in app.instances:
                if not inst.deployed:
                    self.first_fit(inst, machines)

    def pick_instance(self):
        for inst, machine in self.pickFrom.items():
            now_machine = inst.machine
            machine.remove_inst(inst)
            inst.machine = now_machine
            inst.deployed = True

    def fit(self):
        print("using CPU_UTIL_LARGE: %.2f and CPU_UTIL_SMALL: %.2f" % (self.machines[0].CPU_UTIL_THRESHOLD,
                                                                       self.machines[3000].CPU_UTIL_THRESHOLD))
        # self.resolve_init_conflict(self.machines)
        self.migrate_high_cpu_util(self.machines)
        self.pick_instance()
        # self.fit_large_inst(self.machines)
        # self.fit_all(self.machines)

        undeployed_cnt = len(Instance.get_undeployed_insts(self.instances))
        if undeployed_cnt > 0:
            print("Failed: undeployed_inst_count is %d of [%d]" % (undeployed_cnt, len(self.instances)))

    def write_to_csv(self):
        total_score = Machine.total_score(self.machines)
        machine_cnt = Machine.used_machine_count(self.machines)
        print("Score: %.2f, Used machines: %d" % (total_score, machine_cnt))
        s = ("%.2f_%dm" % (total_score, machine_cnt)).replace(".", "_")
        write_to_submit_csv("submit_%s.csv" % s, self.submit_result)
        write_to_search("search-result/search_%s" % (s), self.machines)
