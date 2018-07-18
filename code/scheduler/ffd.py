import numpy as np

import models
import time

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

    def fit_before(self):
        print "starting fit_before"
        time.sleep(2)
        for machine in self.machines:
            # print "\n", machine.id, len(machine.insts)
            # for app in machine.insts:
            #     print app.id,
            # print len(machine.insts)
            for inst in machine.insts.values():

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

    def fit(self):
        print "starting fit"
        time.sleep(2)
        for app in self.applications:
            for inst in app.instances:
                if not inst.placed:
                    self.deploy_inst(inst)
        # for count, machine in enumerate(self.machines):
        #     print machine.id, machine.disk_capacity

    def deploy_inst(self, inst):
        for machine in self.machines:
            if machine.can_deploy_inst(inst):
                machine.put_inst(inst)
                self.submit_result.append((inst.id, machine.id))
                inst.placed = True
                break

    def start_analyse(self, insts, instance_index, file_in):
        LINE_SIZE = 98
        SEARCH_FILE = file_in
        machine_count = 0
        with open(SEARCH_FILE, "r") as f:
            for line in f:
                self.machines[machine_count].apps_id[:] = []
                self.machines[machine_count].mem_use = np.zeros(int(LINE_SIZE))
                self.machines[machine_count].cpu_use = np.zeros(int(LINE_SIZE))
                self.machines[machine_count].disk_use = 0
                self.machines[machine_count].p_num = 0
                self.machines[machine_count].m_num = 0
                self.machines[machine_count].pm_num = 0
                self.machines[machine_count].insts.clear()
                instances_id = line.split()[2].strip('(').strip(')').split(',')
                # print instances_id
                for inst_id in instances_id:
                    if inst_id == '':
                        continue
                    index = instance_index[inst_id]
                    self.machines[machine_count].put_inst(insts[index])
                machine_count += 1
                # print self.machines[machine_count].disk_capacity
        self.fit_before()

