import numpy as np

import models
import time

class FFD(object):
    def __init__(self, instances, applications, machines, app_interfers, machine_index):
        self.instances = instances
        self.machines = machines
        self.applications = applications
        self.app_interfers = app_interfers
        self.machine_index = machine_index

        self.app_id_index = {}
        self.init_deploy_conflict = []
        self.submit_result = []

        self.get_app_id_index()
        self.prepare()
        self.get_deployed_inst()

        self.applications.sort(key=lambda x: x.disk, reverse=True)
        self.machines.sort(key=lambda x: x.disk_capacity, reverse=True)

    def get_app_id_index(self):
        for app in self.applications:
            self.app_id_index[app.id] = app

    def prepare(self):
        for app_interfer in self.app_interfers:
            self.app_id_index[app_interfer.app_a].interfer_others[app_interfer.app_b] = app_interfer.num
            self.app_id_index[app_interfer.app_b].interfer_by_others.append(app_interfer.app_a)

    def get_deployed_inst(self):
        for inst in self.instances:
            # print inst.machine_id
            if inst.machine_id == '':
                continue
            machine_index = self.machine_index[inst.machine_id]
            self.machines[machine_index].insts.append(inst)
            self.machines[machine_index].disk_use += inst.app.disk
            self.machines[machine_index].cpu_use += inst.app.cpu
            self.machines[machine_index].mem_use += inst.app.mem
            self.machines[machine_index].apps_id.append(inst.app.id)
            inst.placed = True

    def fit_before(self):
        print "starting fit_before"
        # self.submit_result[:] = []
        # self.init_deploy_conflict[:] = []
        for machine in self.machines:
            # print "\n", machine.id, len(machine.insts)
            # for app in machine.insts:
            #     print app.id,
            # print len(machine.insts)
            for inst in machine.insts:

                for inst_b in machine.insts:
                    if machine.apps_id.count(inst.app.id) <= 0:
                        break
                    # if inst.app.id == inst_b.app.id:
                    #     if inst.app.interfer_others.has_key(inst_b.app.id) and \
                    #             inst.app.interfer_others[inst_b.app.id] + 1 < machine.apps_id.count(inst_b.app.id):
                    #         # todo: need to move inst
                    #         self.init_deploy_conflict.append("%s, appA:%s %s, appB:%s %s, interfer:%d, deployed:%d" %
                    #                                          (machine.id, inst.app.id, inst.id, inst_b.app.id, inst_b.id,
                    #                                           inst.app.interfer_others[inst_b.app.id],
                    #                                           machine.apps_id.count(inst_b.app.id)))
                    #         print "%s, appA:%s, appB:%s, interfer:%d, deployed:%d" % \
                    #                                          (machine.id, inst.app.id, inst_b.app.id,
                    #                                           inst.app.interfer_others[inst_b.app.id],
                    #                                           machine.apps_id.count(inst_b.app.id))
                    #         print machine.apps_id
                    #         machine.apps_id.remove(inst_b.app.id)
                    #         print machine.apps_id
                    #         machine.mem_use -= inst_b.app.mem
                    #         machine.cpu_use -= inst_b.app.cpu
                    #         machine.disk_use -= inst_b.app.disk
                    #         machine.insts.remove(inst_b)
                    #         self.deploy_inst(inst_b, inst_b.app)
                    #         continue
                    #         pass
                    #
                    # else:
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
                        machine.apps_id.remove(inst_b.app.id)
                        machine.mem_use -= inst_b.app.mem
                        machine.cpu_use -= inst_b.app.cpu
                        machine.disk_use -= inst_b.app.disk
                        machine.insts.remove(inst_b)
                        self.deploy_inst(inst_b, inst_b.app)
                        # i += 1
                        # print "after deployed i = %d" % i
                        continue
                        pass
                    for inst_interferd in machine.insts:
                        if machine.insts.count(inst) < 1:
                            break
                        if inst_interferd.app.id != inst.app.id and \
                                inst_interferd.app.interfer_others.has_key(inst.app.id) and \
                                inst_interferd.app.interfer_others[inst.app.id] < machine.apps_id.count(inst.app.id):
                            # print "dgfsdgds"
                            # todo: need to move inst
                            self.init_deploy_conflict.append("%s, appA:%s %s, appB:%s %s, interfer:%d, deployed:%d" %
                                                             (machine.id, inst_interferd.app.id, inst_interferd.id, inst.app.id,
                                                              inst.id, inst_interferd.app.interfer_others[inst.app.id],
                                                              machine.apps_id.count(inst.app.id)))
                            print "%s, inst was interferd appA:%s, appB:%s, interfer:%d, deployed:%d" % \
                                                             (machine.id, inst_interferd.app.id, inst.app.id,
                                                              inst_interferd.app.interfer_others[inst.app.id],
                                                              machine.apps_id.count(inst.app.id))
                            machine.apps_id.remove(inst.app.id)
                            machine.mem_use -= inst.app.mem
                            machine.cpu_use -= inst.app.cpu
                            machine.disk_use -= inst.app.disk
                            machine.insts.remove(inst)
                            self.deploy_inst(inst, inst.app)
                            # i += 1
                            # print "after deployed i = %d" % i
                            continue
                            pass

            pass

    def fit(self):
        print "starting fit"
        # time.sleep(3)
        for count, app in enumerate(self.applications):
            # print app.id, app.disk, app.interfer_others, app.interfer_by_others
            # if count > 1000:
            #     break
            for inst in app.instances:
                if not inst.placed:
                    self.deploy_inst(inst, app)
        # for count, machine in enumerate(self.machines):
        #     print machine.id, machine.disk_capacity

    def deploy_inst(self, inst, app):
        for machine in self.machines:
            if machine.disk_capacity - machine.disk_use < app.disk:
                continue
            # this machine can hold the instance in disk view
            if (machine.cpu - machine.cpu_use < app.cpu).any():
                continue
            # this machine can hold the instance in cpu view
            if (machine.mem - machine.mem_use < app.mem).any():
                # print machine.mem - machine.mem_use, app.mem
                continue
            # this machine can hold the instance in memory view
            for app_a in app.interfer_by_others:
                if machine.apps_id.count(app_a) > 0:  # already deployed app_a on machine
                    # if app_a == app.id:
                    #     if self.app_id_index[app_a].interfer_others.has_key(app.id) and \
                    #             self.app_id_index[app_a].interfer_others[app.id] + 1 > machine.apps_id.count(app.id):
                    #         pass
                    #     else:
                    #         print "%s %s %s %s interfer:%s deployed:%s" % (
                    #             machine.id, app_a, app.id, inst.id, app.interfer_others[app_a],
                    #             machine.apps_id.count(app_a))
                    #         break
                    # else:
                    if self.app_id_index[app_a].interfer_others.has_key(app.id) and \
                            self.app_id_index[app_a].interfer_others[app.id] > machine.apps_id.count(app.id):

                        pass
                    # todo: app will influence other applications beside app_a
                    else:
                        print "%s %s %s %s interfer:%s deployed:%s" % (
                            machine.id, app_a, app.id, inst.id, self.app_id_index[app_a].interfer_others[app.id],
                            machine.apps_id.count(app.id))
                        break
                            # flag = False
                            # for app_be_interferd in machine.apps_id:
                            #     if app.interfer_others.has_key(app_be_interferd) and \
                            #             app.interfer_others[app_be_interferd] >= machine.apps_id.count(app_be_interferd):
                            #         print "%s %s %s %s interfer:%s deployed:%s" % (
                            #             machine.id, app.id, app_be_interferd, inst.id, app.interfer_others[app_a],
                            #             machine.apps_id.count(app_a))
                            #         pass
                            #     else:
                            #         flag = True
                            #         break
                            # if flag:
                            #     break

            else:
                for app_b in app.interfer_others:
                    if machine.apps_id.count(app_b) > 0:  # already deployed app_b on machine
                        # if app_b == app.id:
                        #     if app.interfer_others[app_b] + 1 > machine.apps_id.count(app_b):
                        #         pass
                        #     else:
                        #         print "%s %s %s %s interfer:%s deployed:%s" % (
                        #             machine.id, app.id, app_b, inst.id, app.interfer_others[app_b],
                        #             machine.apps_id.count(app_b))
                        #         break
                        # else:
                        if app.interfer_others[app_b] > machine.apps_id.count(app_b):

                            pass
                        else:
                            print "%s %s %s %s interfer:%s deployed:%s" % (
                                machine.id, app.id, app_b, inst.id, app.interfer_others[app_b],
                                machine.apps_id.count(app_b))
                            break
                else:
                    # print "deploy {0} on {1}, ".format(inst.id, machine.id)
                    machine.disk_use += app.disk
                    machine.cpu_use += app.cpu
                    machine.mem_use += app.mem
                    machine.insts.append(inst)  # record application whose instance was deployed to this machine
                    machine.apps_id.append(app.id)
                    # print "%s to %s" % (inst.id, machine.id)
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
                self.machines[machine_count].insts[:] = []
                instances_id = line.split()[2].strip('(').strip(')').split(',')
                print instances_id
                for inst_id in instances_id:
                    index = instance_index[inst_id]
                    self.machines[machine_count].apps_id.append(insts[index].app.id)
                    self.machines[machine_count].mem_use += insts[index].app.mem
                    self.machines[machine_count].cpu_use += insts[index].app.cpu
                    self.machines[machine_count].disk_use += insts[index].app.disk
                    self.machines[machine_count].insts.append(insts[index])
                machine_count += 1
        self.fit_before()

