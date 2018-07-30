import copy
import random
import math
import numpy as np


class Search(object):
    def __init__(self, insts, apps, machines, app_interfers):
        self.insts = insts
        self.machines = machines
        self.bak_machines = copy.deepcopy(machines)
        self.apps = apps
        self.app_interfers = app_interfers

        self.machines = sorted(self.machines, key=lambda x: x.disk_capacity, reverse=True)
        self.app_to_interfer = {}
        self.id_to_inst = {}

        self.total_score = 0

        for inst in self.insts:
            self.id_to_inst[inst.id] = inst
        for interfer in self.app_interfers:
            self.app_to_interfer[(interfer.app_a, interfer.app_b)] = interfer
        for i in range(len(self.machines)):
            self.machines[i].app_interfers = self.app_to_interfer

    def rating(self, output):
        for i, line in enumerate(open(output)):
            if line.startswith("undeployed"):
                continue
            insts = line.split()[2][1:-1].split(',')

            cpu = np.zeros(98)
            for inst_id in insts:
                self.machines[i].put_inst(self.id_to_inst[inst_id])
                self.id_to_inst[inst_id].machine_id = i
                cpu += self.id_to_inst[inst_id].app.cpu
        self._rating()

    def _rating(self):
        disk_overload_cnt = 0
        cpu_overload_cnt = 0
        mem_overload_cnt = 0
        p_overload_cnt = 0
        m_overload_cnt = 0
        pm_overload_cnt = 0
        interfer_cnt = 0
        cpu_score = 0

        total_cnt = 0
        violate_cnt = 0

        for machine in self.machines:
            app_dict = {}
            cpu = np.array([0.0] * 98)
            mem = np.array([0.0] * 98)
            disk = 0
            p = 0
            m = 0
            pm = 0

            if machine.disk_use > 0:
                total_cnt += 1

            cpu_score += machine.cpu_score

            for inst in machine.insts.values():
                cpu += inst.app.cpu
                mem += inst.app.mem
                disk += inst.app.disk
                p += inst.app.p
                m += inst.app.m
                pm += inst.app.pm
                if inst.app.id not in app_dict:
                    app_dict[inst.app.id] = []
                app_dict[inst.app.id].append(inst.id)

            for app_a in app_dict.keys():
                for app_b in app_dict.keys():
                    if (app_a, app_b) in self.app_to_interfer:
                        if len(app_dict[app_b]) > self.app_to_interfer[(app_a, app_b)].num:
                            interfer_cnt += 1
                            violate_cnt += len(app_dict[app_b])
                    if (app_b, app_a) in self.app_to_interfer:
                        if len(app_dict[app_a]) > self.app_to_interfer[(app_b, app_a)].num:
                            interfer_cnt += 1
                            violate_cnt += len(app_dict[app_a])

            if disk > machine.disk_capacity:
                disk_overload_cnt += 1
            if any(cpu > machine.cpu_capacity):
                cpu_overload_cnt += 1
            if any(mem > machine.mem_capacity):
                mem_overload_cnt += 1
            if p > machine.p_capacity:
                p_overload_cnt += 1
            if m > machine.m_capacity:
                m_overload_cnt += 1
            if pm > machine.pm_capacity:
                pm_overload_cnt += 1

        print "CPU Score: %d" % cpu_score
        print "Disk Overload: %f (%d / %d)" % (float(disk_overload_cnt) / total_cnt, disk_overload_cnt, total_cnt)
        print "CPU Overload: %f (%d / %d)" % (float(cpu_overload_cnt) / total_cnt, cpu_overload_cnt, total_cnt)
        print "Memory Overload: %f (%d / %d)" % (float(mem_overload_cnt) / total_cnt, mem_overload_cnt, total_cnt)
        print "P Overload %f (%d / %d)" % (float(p_overload_cnt) / total_cnt, p_overload_cnt, total_cnt)
        print "M Overload %f (%d / %d)" % (float(m_overload_cnt) / total_cnt, m_overload_cnt, total_cnt)
        print "PM Overload %f (%d / %d)" % (float(pm_overload_cnt) / total_cnt, pm_overload_cnt, total_cnt)
        print "Constraint violate: %d / %d" % (interfer_cnt, violate_cnt)

    def search(self):
        self.total_score = sum([i.cpu_score for i in self.machines])

        while True:
            if not self._search():
                break

    def _search(self):
        set1 = range(len(self.machines))
        set2 = range(len(self.machines))
        random.shuffle(set1)
        random.shuffle(set2)

        for i in set1:
            for j in set2:
                if i == j:
                    continue

                choice = self.choice()
                if choice == 1:
                    for inst in self.machines[j].insts.values():
                        if self.can_move_inst(self.machines[i], inst, i):
                            print "move %s -> %s: %f" % (inst.id, self.machines[i].id, self.total_score)
                            return True
                elif choice == 2:
                    for inst1 in self.machines[i].insts.values():
                        for inst2 in self.machines[j].insts.values():
                            if self.can_swap_inst(inst1, inst2):
                                print "swap %s <-> %s: %f" % (inst1.id, inst2.id, self.total_score)
                                return True
        return False

    def choice(self):
        rand = random.random()
        if rand < 0.4:
            return 1
        else:
            return 2

    def can_move_inst(self, machine1, inst, machine_id_of_machine1):
        if machine1.disk_use == 0:
            return False

        machine2 = self.machines[inst.machine_id]
        resource = machine1.resource_use + inst.app.resource
        if any(np.around(resource, 8) > np.around(machine1.resource_capacity)):
            return False

        score_before = machine1.cpu_score + machine2.cpu_score

        machine_id_of_inst = inst.machine_id
        inst.machine_id = machine_id_of_machine1
        machine2.remove_inst(inst)
        machine1.put_inst(inst)

        if machine1.has_conflict():
            inst.machine_id = machine_id_of_inst
            machine1.remove_inst(inst)
            machine2.put_inst(inst)
            return False

        score_after = machine1.cpu_score + machine2.cpu_score
        if score_after > score_before:
            inst.machine_id = machine_id_of_inst
            machine1.remove_inst(inst)
            machine2.put_inst(inst)
            return False

        self.total_score = self.total_score - score_before + score_after
        return True

    def can_swap_inst(self, inst1, inst2):
        if inst1.id not in self.machines[inst1.machine_id].insts:
            return False
        if inst2.id not in self.machines[inst2.machine_id].insts:
            return False

        machine1 = self.machines[inst1.machine_id]
        machine2 = self.machines[inst2.machine_id]

        resource1 = machine1.resource_use - inst1.app.resource + inst2.app.resource
        resource2 = machine2.resource_use - inst2.app.resource + inst1.app.resource

        if any(np.around(resource1, 8) > np.around(machine1.resource_capacity)) or any(
                np.around(resource2, 8) > np.around(machine2.resource_capacity, 8)):
            return False

        score1 = machine1.cpu_score
        score2 = machine2.cpu_score

        self.do_swap(inst1, inst2)

        if machine1.has_conflict() or machine2.has_conflict():
            self.do_swap(inst1, inst2)
            return False

        delta = score1 + score2 - machine1.cpu_score - machine2.cpu_score
        if delta <= 0:
            self.do_swap(inst1, inst2)
            return False

        self.total_score = self.total_score - score1 - score2 + machine1.cpu_score + machine2.cpu_score
        return True

    def do_swap(self, inst1, inst2):
        machine1 = self.machines[inst1.machine_id]
        machine2 = self.machines[inst2.machine_id]
        tmp = inst1.machine_id
        inst1.machine_id = inst2.machine_id
        inst2.machine_id = tmp
        machine1.put_inst(inst2)
        machine1.remove_inst(inst1)
        machine2.put_inst(inst1)
        machine2.remove_inst(inst2)

    def output(self):
        self.machines.sort(key=lambda x: x.disk_capacity, reverse=True)
        for machine in self.machines:
            if machine.resource_overload():
                print machine.resource_use > machine.resource_capacity
                print machine.resource_capacity - machine.resource_use
                print "Invalid search result: resource overload"
                return
            if machine.has_conflict():
                print "invalid search result: constraint conflict"
                return

        path = "search-result/search_%d_%dm" % (
        int(math.ceil(self.total_score)), len(filter(lambda x: x.disk_use > 0, self.machines)))
        with open(path, 'w') as f:
            print "writing to path %s" % path
            for machine in self.machines:
                if machine.disk_use == 0:
                    continue
                line = "total(%f,%d): {%s} (%s)\n" % (
                    machine.cpu_score, machine.disk_use,
                    ",".join([str(int(i.app.disk)) for i in machine.insts.values()]),
                    ",".join([i.id for i in machine.insts.values()]))
                f.write(line)
