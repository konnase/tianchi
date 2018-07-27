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
            # print inst.machine_id
            if inst.machine_id == '':
                continue
            machine_index = self.machine_index[inst.machine_id]
            self.machines[machine_index].put_inst(inst)
            inst.placed = True

    def fit_before(self, machines):
        print "starting fit_before"
        time.sleep(2)

        for machine in machines:
            # print "\n", machine.id, len(machine.insts)
            # for app in machine.insts:
            #     print app.id,
            # print len(machine.insts)
            for inst in machine.insts.values():
                if machine.disk_capacity > 2400 and (machine.cpu_use > machine.cpu_capacity * 0.7).any():
                    machine.remove_inst(inst)
                    self.deploy_inst(inst)
                if machine.disk_capacity < 2400 and (machine.cpu_use > machine.cpu_capacity * 0.6).any():
                    machine.remove_inst(inst)
                    self.deploy_inst(inst)

                for inst_b in machine.insts.values():
                    if machine.apps_id.count(inst.app.id) <= 0:
                        break
                    if inst.app.interfer_others.has_key(inst_b.app.id) and \
                            inst.app.interfer_others[inst_b.app.id] < machine.apps_id.count(inst_b.app.id):
                        # todo: need to move inst
                        self.init_deploy_conflict.append("%s, appA:%s %s, appB:%s %s, interfer:%d, deployed:%d" %
                                                         (machine.id, inst.app.id, inst.id, inst_b.app.id,
                                                          inst_b.id, inst.app.interfer_others[inst_b.app.id],
                                                          machine.apps_id.count(inst_b.app.id)))
                        print "%s, appA:%s, appB:%s, interfer:%d, deployed:%d" % \
                                                         (machine.id, inst.app.id, inst_b.app.id,
                                                          inst.app.interfer_others[inst_b.app.id],
                                                          machine.apps_id.count(inst_b.app.id))
                        machine.remove_inst(inst_b)
                        self.deploy_inst(inst_b)
                        # i += 1
                        # print "after deployed i = %d" % i
                        pass
                for inst_interferd in machine.insts.values():
                    if not machine.insts.has_key(inst):
                        break
                    if inst_interferd.app.interfer_others.has_key(inst.app.id) and \
                            inst_interferd.app.interfer_others[inst.app.id] < machine.apps_id.count(inst.app.id):
                        # todo: need to move inst
                        self.init_deploy_conflict.append("%s, appA:%s %s, appB:%s %s, interfer:%d, deployed:%d" %
                                                         (machine.id, inst_interferd.app.id, inst_interferd.id, inst.app.id,
                                                          inst.id, inst_interferd.app.interfer_others[inst.app.id],
                                                          machine.apps_id.count(inst.app.id)))
                        print "%s, inst was interferd appA:%s, appB:%s, interfer:%d, deployed:%d" % \
                                                         (machine.id, inst_interferd.app.id, inst.app.id,
                                                          inst_interferd.app.interfer_others[inst.app.id],
                                                          machine.apps_id.count(inst.app.id))
                        machine.remove_inst(inst)
                        self.deploy_inst(inst)
                        pass

            pass

    def fit_large_inst(self):
        print "starting fit_large_inst"
        time.sleep(2)
        for app in self.applications:
            for inst in app.instances:
                if app.disk >= LARGE_DISK_INST or (app.cpu >= np.full(int(LINE_SIZE), LARGE_CPU_INST)).any() \
                        or (app.mem >= np.full(int(LINE_SIZE), LARGE_MEM_INST)).any() and not inst.placed:
                    for machine in self.machines:
                        if machine.disk_capacity >= 2400 and machine.disk_use == 0:
                            machine.put_inst(inst)
                            self.submit_result.append((inst.id, machine.id))
                            inst.placed = True
                            print "deployed %s of %s on %s" % (inst.id, inst.app.id, machine.id)
                            break

    def fit(self):
        self.fit_before(self.machines)
        self.fit_large_inst()
        print "starting fit"
        time.sleep(2)
        for app in self.applications:
            for inst in app.instances:
                if not inst.placed:
                    self.deploy_inst(inst)
        self.fit_before(self.machines)
        # for count, machine in enumerate(self.machines):
        #     print machine.id, machine.disk_capacity

    def deploy_inst(self, inst):
        for machine in self.machines:
            if machine.can_deploy_inst(inst):
                machine.put_inst(inst)
                self.submit_result.append((inst.id, machine.id))
                inst.placed = True
                print "deployed %s of %s on %s" % (inst.id, inst.app.id, machine.id)
                break


