import datetime
import math

import numpy as np

import models
import time

from constant import *


class FFD(object):
    def __init__(self, instances, applications, machines, app_interfers, machine_index, app_index):
        self.instances = instances
        self.machines = machines
        self.applications = applications
        self.app_interfers = app_interfers
        self.machine_index = machine_index
        self.app_index = app_index

        self.app_id_index = {}
        self.init_deploy_conflict = []
        self.submit_result = []

        self.get_app_id_index()
        self.get_deployed_inst()

        self.applications.sort(key=lambda x: x.disk, reverse=True)
        self.machines.sort(key=lambda x: x.disk_capacity, reverse=True)

    def get_app_id_index(self):
        for app in self.applications:
            self.app_id_index[app.id] = app

    def get_deployed_inst(self):
        for inst in self.instances:
            if inst.machine_id == '':
                continue
            machine_index = self.machine_index[inst.machine_id]
            self.machines[machine_index].put_inst(inst)
            inst.placed = True

    def resolve_init_conflict(self, machines):
        print "starting resolve_init_conflict"
        time.sleep(2)

        for machine in machines:
            for inst in machine.insts.values():
                if machine.is_cpu_util_too_high():
                    machine.remove_inst(inst)
                    self.deploy_inst(inst)
                    continue
                for inst_b in machine.insts.values():
                    if machine.apps_id.count(inst.app.id) <= 0:
                        break
                    if machine.has_init_conflict(inst, inst_b):
                        self.record_init_conflict(machine, inst, inst_b)
                        machine.remove_inst(inst_b)
                        self.deploy_inst(inst_b)
                for inst_interferd in machine.insts.values():
                    if not machine.insts.has_key(inst):
                        break
                    if machine.has_init_conflict(inst_interferd, inst):
                        self.record_init_conflict(machine, inst_interferd, inst)
                        machine.remove_inst(inst)
                        self.deploy_inst(inst)

    def fit_large_inst(self):
        print "starting fit_large_inst"
        time.sleep(2)
        for app in self.applications:
            for inst in app.instances:

                if not inst.placed and (
                        app.disk >= LARGE_DISK_INST or (app.cpu >= np.full(int(LINE_SIZE), LARGE_CPU_INST)).any()
                        or (app.mem >= np.full(int(LINE_SIZE), LARGE_MEM_INST)).any()):
                    for machine in self.machines:
                        if machine.disk_capacity == DISK_CAP_LARGE and machine.disk_use <= 200:
                            if machine.can_deploy_inst(inst):
                                self.deploy_inst_on_machine(machine, inst)
                                print "deployed %s of %s on %s" % (inst.id, inst.app.id, machine.id)
                                break

    def fit(self):
        print global_var.CPU_UTIL_LARGE
        self.fit_large_inst()
        self.resolve_init_conflict(self.machines)
        print "starting fit"
        time.sleep(2)
        for app in self.applications:
            for inst in app.instances:
                if not inst.placed:
                    self.deploy_inst(inst)
        self.resolve_init_conflict(self.machines)

    def deploy_inst(self, inst):
        for machine in self.machines:
            if machine.can_deploy_inst(inst):
                self.deploy_inst_on_machine(machine, inst)
                print "deployed %s of %s on %s" % (inst.id, inst.app.id, machine.id)
                break
        else:
            raise Exception("%s is not deployed" % inst)

    def deploy_inst_on_machine(self, machine, inst):
        machine.put_inst(inst)
        self.submit_result.append((inst.id, machine.id))

    def record_init_conflict(self, machine, inst, inst_b):
        self.init_deploy_conflict.append("%s, appA:%s %s, appB:%s %s, interfer:%d, deployed:%d" %
                                         (machine.id, inst.app.id, inst.id, inst_b.app.id, inst_b.id,
                                          inst.app.interfer_others[inst_b.app.id],
                                          machine.apps_id.count(inst_b.app.id)))
        print "%s, appA:%s, appB:%s, interfer:%d, deployed:%d" % \
              (machine.id, inst.app.id, inst_b.app.id, inst.app.interfer_others[inst_b.app.id],
               machine.apps_id.count(inst_b.app.id))

    def write_to_csv(self):
        with open("init_deploy_conflict.csv", "w") as f:
            for count, item in enumerate(self.init_deploy_conflict):
                f.write("{0}\n".format(item))
        with open("submit.csv", "w") as f:
            for count, item in enumerate(self.submit_result):
                f.write("{0},{1}\n".format(item[0], item[1]))
        z = datetime.datetime.now()
        final_score = 0
        with open("search-result/search_%s%s_%s%s" % (z.month, z.day, z.hour, z.minute), "w") as f:
            for count, machine in enumerate(self.machines):
                inst_disk = ""
                inst_id = ""
                all_disk_use = 0
                score = 0
                for i in range(98):
                    score += (1 + 10 * (math.exp(max(0, (machine.cpu_use[i] / machine.cpu_capacity) - 0.5)) - 1))
                for inst in machine.insts.values():
                    inst_disk += "," + str(inst.app.disk)
                    inst_id += "," + str(inst.id)
                    all_disk_use += inst.app.disk
                if all_disk_use == 0:
                    continue
                final_score += score / 98
                f.write("total(%s,%s): {%s}, (%s)\n" % (
                    score / 98, all_disk_use, inst_disk.lstrip(','), inst_id.lstrip(',')))
        print final_score
