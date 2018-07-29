import numpy as np
import re
import copy
import math
from itertools import combinations

# todo: fix this

class Knapsack(object):
    def __init__(self, insts, apps, machines, app_interfers):
        self.insts = insts
        self.machines = machines
        self.bak_machines = copy.deepcopy(machines)
        self.apps = apps
        self.app_interfers = app_interfers

        self.inst_index = {}
        self.inst_id_index = {}
        self.interfer_index = {}
        self.bins = []

        self.reverse = dict()
        self.safe_apps = set()

        self.pre_process_machine()
        self.build_index()

        self.done = set()
        self.machine_dict = {}
        self.bak_machine_dict = {}
        self.done = 0

    def print_request(self):
        with open('request', 'w') as f:
            f.write("%d\n" % len(self.insts))
            for inst in self.insts:
                line = "%d %d %d %s\n" % (
                int(math.ceil(max(inst.app.cpu))), int(math.ceil(max(inst.app.mem))), inst.app.disk, inst.id)
                f.write(line)

    def build_index(self):
        inst_index = {}
        inst_id_index = {}
        self.insts = sorted(self.insts, key=lambda x: x.score, reverse=True)

        for inst in self.insts:
            inst_id_index[inst.id] = inst
            if inst.app.disk not in inst_index:
                inst_index[inst.app.disk] = list()
            inst_index[inst.app.disk].append(inst)

        for interfer in self.app_interfers:
            self.interfer_index[(interfer.app_a, interfer.app_b)] = interfer

        for app in self.apps:
            if app.id not in self.interfer_index and app.id not in self.reverse:
                self.safe_apps.add(app.id)

        self.inst_index = inst_index
        self.inst_id_index = inst_id_index

    def pre_process_machine(self):
        self.machines = sorted(self.machines, key=lambda x: x.disk_capacity, reverse=True)

    def read_lower_bound(self):
        for i, line in enumerate(open("lower_bound")):
            self.machines[i].bins = map(int, re.findall(r'[^{}]+', line)[1].split(','))

    @staticmethod
    def place_score(result, machine):
        size = len(result[0].app.cpu)
        cpu = np.zeros(size)
        mem = np.zeros(size)

        for item in result:
            cpu += item.app.cpu
            mem += item.app.mem

        return np.linalg.norm(cpu / machine.cpu, ord=1) / size, np.linalg.norm(mem / machine.mem, ord=1) / size

    @staticmethod
    def score(weight):
        pass

    def pre_process(self):
        for inst in self.insts:
            print int(round(max(inst.app.cpu))), int(round(max(inst.app.mem))), int(inst.app.disk), inst.id

    def test(self, output):
        for i, line in enumerate(open(output)):
            if line.startswith("undeployed"):
                continue
            insts = line.split()[2][1:-1].split(',')

            for inst in insts:
                self.machines[i].put_inst(self.inst_id_index[inst])
                self.inst_id_index[inst].machine_id = i
        self.rating()

    def handle_swap_500_100(self):
        candidates_1 = []
        for i in range(len(self.machines)):
            inst_60 = 0
            if self.machines[i].disk_capacity == 1024.0 and self.machines[i].cpu_score < 0.2:
                for inst in self.machines[i].insts.values():
                    if inst.app.disk == 60:
                        inst_60 += 1
                    if inst_60 >= 1:
                        candidates_1.append(i)
                        break
        candidates_1.sort(key=lambda x: self.machines[x].cpu_score)
        candidates_2 = []
        for i in range(len(self.machines)):
            if self.machines[i].disk_capacity != 600 or self.machines[i].cpu_score < 0.5:
                continue
            inst_500 = 0
            inst_100 = 0
            for inst in self.machines[i].insts.values():
                if inst.app.disk == 500:
                    inst_500 += 1
                if inst.app.disk == 100:
                    inst_100 += 1
                if inst_500 == 1 and inst_100 == 1:
                    candidates_2.append(i)
                    break
                candidates_2.sort(key=lambda x: self.machines[x].cpu_score)

        for i in range(3):
            machine1 = copy.copy(self.machines[candidates_1[i]])
            machine2 = copy.copy(self.machines[candidates_2[i]])

            print machine1
            print machine2

            insts2 = machine2.insts.values()
            insts1 = sorted(filter(lambda x: x.app.disk == 60, machine1.insts.values()), key=lambda x: max(x.app.cpu))[
                     0:10]

            disk = 0
            cpu = np.array([0.0] * 98)
            for inst in insts1:
                cpu += inst.app.cpu
                disk += inst.app.disk

            self.score_after_swap_mul(insts1, insts2)

    def rating(self):
        disk_overload_cnt = 0
        cpu_overload_cnt = 0
        mem_overload_cnt = 0
        half_cpu_overload_cnt = 0
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

            # if 'app_1778' not in app_dict and machine.disk_use + 100 < machine.disk_capacity and machine.disk_use > 0:
            #     print machine

            for app_a in app_dict.keys():
                for app_b in app_dict.keys():
                    if (app_a, app_b) in self.interfer_index:
                        if len(app_dict[app_b]) > self.interfer_index[(app_a, app_b)].num:
                            print "%s: %s and %s -> max(%d) actual(%d)" % (
                                machine.id, app_a, app_b, self.interfer_index[(app_a, app_b)].num, len(app_dict[app_b]))
                            print ",".join(app_dict[app_b])
                            interfer_cnt += 1
                            violate_cnt += len(app_dict[app_b])
                    if (app_b, app_a) in self.interfer_index:
                        if len(app_dict[app_a]) > self.interfer_index[(app_b, app_a)].num:
                            print "%s: %s and %s -> max(%d) actual(%d)" % (
                                machine.id, app_b, app_a, self.interfer_index[(app_b, app_a)].num, len(app_dict[app_a]))
                            print ",".join(app_dict[app_a])
                            interfer_cnt += 1
                            violate_cnt += len(app_dict[app_a])

            if disk > machine.disk_capacity:
                disk_overload_cnt += 1
            if any(cpu > machine.cpu_capacity):
                cpu_overload_cnt += 1
            if any(mem > machine.mem_capacity):
                mem_overload_cnt += 1
            if any(cpu > machine.cpu_capacity / 2):
                half_cpu_overload_cnt += 1
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
        print "Half CPU Overload: %f (%d / %d)" % (
            float(half_cpu_overload_cnt) / total_cnt, half_cpu_overload_cnt, total_cnt)
        print "P Overload %f (%d / %d)" % (float(p_overload_cnt) / total_cnt, p_overload_cnt, total_cnt)
        print "M Overload %f (%d / %d)" % (float(m_overload_cnt) / total_cnt, m_overload_cnt, total_cnt)
        print "PM Overload %f (%d / %d)" % (float(pm_overload_cnt) / total_cnt, pm_overload_cnt, total_cnt)
        print "Constraint violate: %d / %d" % (interfer_cnt, violate_cnt)

    def fix_bug(self):
        for i in range(len(self.machines)):
            if self.machines[i].disk_use <= self.machines[i].disk_capacity:
                continue
            while self.machines[i].disk_use > self.machines[i].disk_capacity:
                for j in range(len(self.machines)):
                    for inst in self.machines[i].insts.values():
                        if self.machines[i].id == self.machines[j].id or self.machines[j].disk_use == 0:
                            continue
                        if self.machines[j].can_put_inst(inst):
                            print "%s %s -> %s" % (inst.id, self.machines[i], self.machines[j])
                            self.machines[i].take_out(inst)
                            self.machines[j].put_inst(inst)

    def search(self):
        self.machines = filter(lambda x: x.disk_use > 0, self.machines)
        self.machines.sort(key=lambda x: x.cpu_score)

        for i in range(len(self.machines)):
            for inst_id in self.machines[i].insts.keys():
                self.machines[i].insts[inst_id].machine_id = i

        while True:
            if not self.handle_disk():
                # if not self.handle_constraint_by_move():
                break

        self.rating()

    def update_pmp(self):
        for i, machine in enumerate(self.machines):
            self.machines[i].pmp = np.array([0] * 3)
            self.machines[i].pmp_cap = np.array(
                [self.machines[i].p_capacity, self.machines[i].m_capacity, self.machines[i].pm_capacity])
            for inst in machine.insts.values():
                self.machines[i].pmp += np.array([inst.app.p, inst.app.m, inst.app.pm])

    def handle_pm(self):
        self.update_pmp()
        set1 = filter(lambda x: any(x.pmp > x.pmp_cap), self.machines)
        set2 = filter(lambda x: all(x.pmp < x.pmp_cap) and x.disk_capacity == 1024, self.machines)
        print len(set1), len(set2)

        for i, machine1 in enumerate(set1):
            print "SET1(%d/%d) %s" % (i + 1, len(set1), set1[i])
            for inst1 in machine1.insts.values():
                if inst1.app.disk != 60:
                    continue
                for machine2 in set2:
                    for inst2 in machine2.insts.values():
                        if inst2.app.disk != 60:
                            continue
                        if self.can_swap_pmp(inst1, inst2):
                            return True
        return False

    def can_swap_pmp(self, inst1, inst2):
        machine1 = copy.copy(self.machines[inst1.machine_id])
        machine2 = copy.copy(self.machines[inst2.machine_id])

        machine1.cpu_use = machine1.cpu_use - inst1.app.cpu + inst2.app.cpu
        machine2.cpu_use = machine2.cpu_use - inst2.app.cpu + inst1.app.cpu
        if any(machine1.cpu_use > machine1.cpu_capacity) or any(machine2.cpu_use > machine2.cpu_capacity):
            return False

        machine1.mem_use = machine1.mem_use - inst1.app.mem + inst2.app.mem
        machine2.mem_use = machine2.mem_use - inst2.app.mem + inst1.app.mem
        if any(machine1.mem_use > machine1.mem_capacity) or any(machine2.mem_use > machine2.mem_capacity):
            return False

        machine1.pmp = machine1.pmp - np.array([inst1.app.p, inst1.app.m, inst1.app.pm]) + np.array(
            [inst2.app.p, inst2.app.m, inst2.app.pm])
        machine2.pmp = machine2.pmp - np.array([inst2.app.p, inst2.app.m, inst2.app.pm]) + np.array(
            [inst1.app.p, inst1.app.m, inst1.app.pm])

        if sum(machine1.pmp) < sum(self.machines[inst1.machine_id].pmp) and all(machine2.pmp < machine2.pmp_cap):
            print machine1.pmp, self.machines[inst1.machine_id].pmp
            print machine2.pmp, self.machines[inst2.machine_id].pmp
            self.do_swap(inst1, inst2)
            return True
        return False

    def update_app_inst(self):
        for i, machine in enumerate(self.machines):
            self.machines[i].app_interfers = self.interfer_index

    def handle_constraint_violate(self):
        self.update_pmp()
        self.update_app_inst()

        set1 = filter(lambda x: x.inter_inst_num > 0, self.machines)
        set2 = filter(lambda x: x.inter_inst_num == 0, self.machines)

        for i, machine1 in enumerate(set1):
            if set1[i] in self.done:
                continue
            print "SET1(%d/%d) %s" % (i + 1, len(set1), set1[i])
            for inst1 in set1[i].insts.values():
                if inst1.app.id not in set1[i].violate_apps:
                    continue
                print inst1.id
                for j, machine2 in enumerate(set2):
                    for inst2 in set2[j].insts.values():
                        if self.can_swap_constraint(inst1, inst2):
                            return True
            self.done.add(set1[i])
        return False

    def can_swap_constraint(self, inst1, inst2):
        machine1 = copy.copy(self.machines[inst1.machine_id])
        machine2 = copy.copy(self.machines[inst2.machine_id])

        machine1.disk_use = machine1.disk_use - inst1.app.disk + inst2.app.disk
        machine2.disk_use = machine2.disk_use - inst2.app.disk + inst1.app.disk
        if machine1.disk_use > machine1.disk_capacity or machine2.disk_use > machine2.disk_capacity:
            return False

        machine1.cpu_use = machine1.cpu_use - inst1.app.cpu + inst2.app.cpu
        machine2.cpu_use = machine2.cpu_use - inst2.app.cpu + inst1.app.cpu
        if any(machine1.cpu_use > machine1.cpu_capacity / 2) or any(machine2.cpu_use > machine2.cpu_capacity / 2):
            return False

        machine1.mem_use = machine1.mem_use - inst1.app.mem + inst2.app.mem
        machine2.mem_use = machine2.mem_use - inst2.app.mem + inst1.app.mem
        if any(machine1.mem_use > machine1.mem_capacity) or any(machine2.mem_use > machine2.mem_capacity):
            return False

        machine1.pmp = machine1.pmp - np.array([inst1.app.p, inst1.app.m, inst1.app.pm]) + np.array(
            [inst2.app.p, inst2.app.m, inst2.app.pm])
        machine2.pmp = machine2.pmp - np.array([inst2.app.p, inst2.app.m, inst2.app.pm]) + np.array(
            [inst1.app.p, inst1.app.m, inst1.app.pm])
        if any(machine1.pmp > machine1.pmp_cap) or any(machine2.pmp > machine2.pmp_cap):
            return False

        print inst2.id

        violate1 = self.machines[inst1.machine_id].inter_inst_num
        violate2 = self.machines[inst2.machine_id].inter_inst_num

        self.do_swap(inst1, inst2)

        violate1a = self.machines[inst1.machine_id].inter_inst_num
        violate2a = self.machines[inst2.machine_id].inter_inst_num
        # print violate1, violate2, violate1a, violate2a

        # if violate1a < violate1 and violate2a <= violate2:
        if violate1a < violate1 and (violate1a + violate2a) < (violate1 + violate2):
            print "after swap: machine1 cpu(%f) mem(%f), machine2 cpu(%f) mem(%f)" % (
                self.machines[inst1.machine_id].cpu_score, self.machines[inst1.machine_id].mem_score,
                self.machines[inst1.machine_id].cpu_score,
                self.machines[inst1.machine_id].mem_score)
            # print violate1, violate2, violate1a, violate2a
            return True
        self.do_swap(inst2, inst1)
        return False

    def handle_constraint_by_move(self):
        self.update_pmp()
        self.update_app_inst()

        set1 = filter(lambda x: x.inter_inst_num > 0, self.machines)
        set2 = filter(lambda x: x.inter_inst_num == 0, self.machines)

        for i, machine1 in enumerate(set1):
            if set1[i] in self.done:
                continue
            print "SET1(%d/%d) %s" % (i + 1, len(set1), set1[i])
            for inst1 in set1[i].insts.values():
                if inst1.app.id not in set1[i].violate_apps:
                    continue
                for machine2 in set2:
                    if self.can_move_constraint(inst1, machine2):
                        return True

    def handle_disk(self):
        set1 = filter(lambda x: x.disk_use > x.disk_capacity, self.machines)
        set2 = filter(lambda x: x.disk_use <= x.disk_capacity, self.machines)

        for i, machine1 in enumerate(set1):
            print "SET1(%d/%d) %s" % (i + 1, len(set1), set1[i])
            for inst1 in set1[i].insts.values():
                for j, machine2 in enumerate(set2):
                    for inst2 in set2[j].insts.values():
                        if self.can_swap_disk(inst1, inst2):
                            return True
        return False

    def can_swap_disk(self, inst1, inst2):
        machine1 = self.machines[inst1.machine_id]
        machine2 = self.machines[inst2.machine_id]
        disk1 = machine1.disk_use - inst1.app.disk + inst2.app.disk
        disk2 = machine2.disk_use - inst2.app.disk + inst1.app.disk
        delta = machine1.disk_use ** 2 + machine2.disk_use ** 2 - disk1**2 - disk2**2
        if disk2 < machine2.disk_capacity and delta > 0:
            print machine1.disk_use, machine2.disk_use, disk1, disk2, delta
            self.do_swap(inst1, inst2)
            return True
        return False

    def can_move_constraint(self, inst, machine):
        if machine.disk_use + inst.app.disk > machine.disk_capacity:
            return False
        if any((machine.cpu_use + inst.app.cpu) > machine.cpu_capacity / 2):
            return False
        if any((machine.mem_use + inst.app.mem) > machine.mem_capacity):
            return False
        if any((machine.pmp + np.array([inst.app.p, inst.app.m, inst.app.pm])) > machine.pmp_cap):
            return False

        app_dic = copy.copy(machine.app_count)
        app_dic[inst.app.id] = app_dic[inst.app.id] + 1 if inst.app.id in app_dic else 1
        for app1, cnt2 in app_dic.iteritems():
            for app2, cnt2 in app_dic.iteritems():
                if (app1, app2) in self.interfer_index:
                    if cnt2 > self.interfer_index[(app1, app2)].num:
                        return False

        print "put %s to %s" % (inst.id, machine.id)
        for i in range(len(self.machines)):
            if self.machines[i].id == machine.id:
                self.machines[i].put_inst(inst)
                break
        return True

    def step_one_one(self):
        set1 = filter(lambda x: x.cpu_score > 0.5, self.machines)
        set2 = filter(lambda x: x.cpu_score < 0.49, self.machines)

        for i in range(len(set1) - 1, -1, -1):
            if set1[i].id in self.done:
                continue
            print "SET1(%d/%d): " % (i + 1, len(set1)), set1[i]
            for j in range(len(set2)):
                for inst1 in set1[i].insts.values():
                    if inst1.app.disk in [1024.0, 600.0]:
                        continue
                    for inst2 in set2[j].insts.values():
                        if inst2.app.disk in [1024.0, 600.0] or inst2.app.disk != inst1.app.disk:
                            continue
                        if inst1.id != inst2.id and inst1.machine_id != inst2.machine_id:
                            if self.score_after_swap(inst1, inst2):
                                return True
            self.done.add(set1[i].id)

    def step_one_mul(self):
        set1 = filter(lambda x: x.cpu_score > 0.5, self.machines)
        set2 = filter(lambda x: x.cpu_score < 0.45, self.machines)
        self.done.clear()

        set1_dic = {}
        set2_dic = {}

        for i, machine in enumerate(set1):
            for inst in machine.insts.values():
                if inst.app.disk not in set1_dic:
                    set1_dic[inst.app.disk] = set()
                set1_dic[inst.app.disk].add(machine)

        for machine in set2:
            for inst in machine.insts.values():
                if inst.app.disk not in set2_dic:
                    set2_dic[inst.app.disk] = set()
                set2_dic[inst.app.disk].add(machine)

        for (disk_set1, disk_set2, set1_num, set2_num) in [(300, 60, 1, 5), (100, 40, 2, 5), (500, 100, 1, 5),
                                                           (40, 60, 3, 2)]:
            print "search", disk_set1, disk_set2, set1_num, set2_num
            if disk_set1 not in set1_dic or disk_set2 not in set2_dic:
                continue
            for i, machine1 in enumerate(set1_dic[disk_set1]):
                if machine1.id in self.done:
                    continue
                print "SET1(%d/%d): " % (i + 1, len(set1_dic[disk_set1])), machine1
                for machine2 in set2_dic[disk_set2]:
                    if machine1.id == machine2.id:
                        continue
                    if len(filter(lambda x: x.app.disk == disk_set2, machine2.insts.values())) < disk_set1 / disk_set2:
                        continue
                    if self.step_one_mul_search(machine1, machine2, disk_set1, disk_set2, set1_num, set2_num):
                        return True
                self.done.add(machine1.id)

    def step_one_mul_search(self, machine1, machine2, disk_set1, disk_set2, set1_num, set2_num):
        inst_set1 = filter(lambda x: x.app.disk == disk_set1, machine1.insts.values())
        inst_set2 = filter(lambda x: x.app.disk == disk_set2, machine2.insts.values())

        for inst1_comb in list(combinations(inst_set1, set1_num)):
            for inst2_comb in list(combinations(inst_set2, set2_num)):
                if self.score_after_swap_mul(inst1_comb, inst2_comb):
                    return True
        return False

    def score_after_swap_mul(self, inst1_set, inst2_set):
        machine1 = self.machines[inst1_set[0].machine_id]
        machine2 = self.machines[inst2_set[0].machine_id]

        cpu1 = copy.copy(machine1.cpu_use)
        cpu2 = copy.copy(machine2.cpu_use)
        mem1 = copy.copy(machine1.mem_use)
        mem2 = copy.copy(machine2.mem_use)

        for inst in inst1_set:
            cpu1 -= inst.app.cpu
            cpu2 += inst.app.cpu
            mem1 -= inst.app.mem
            mem2 += inst.app.mem

        for inst in inst2_set:
            cpu1 += inst.app.cpu
            cpu2 -= inst.app.cpu
            mem1 += inst.app.mem
            mem2 -= inst.app.mem

        cpu_radio1 = max(cpu1) / machine1.cpu_capacity
        cpu_radio2 = max(cpu2) / machine2.cpu_capacity
        cpu_delta = -cpu_radio1 ** 2 - cpu_radio2 ** 2 + machine1.cpu_score ** 2 + machine2.cpu_score ** 2

        mem_radio1 = max(mem1) / machine1.mem_capacity
        mem_radio2 = max(mem2) / machine2.mem_capacity
        mem_delta = -mem_radio1 ** 2 - mem_radio2 ** 2 + machine1.mem_score ** 2 + machine2.mem_score ** 2

        print cpu_radio1, cpu_radio2, mem_radio1, mem_radio2

        if cpu_radio2 <= 0.5 and mem_radio1 <= 1 and mem_radio2 <= 1 and cpu_delta > 0.01:
            self.do_swap_mul(inst1_set, inst2_set)
            return True
        return False

    def do_swap_mul(self, inst1_set, inst2_set):
        i = inst1_set[0].machine_id
        j = inst2_set[0].machine_id

        for inst in inst1_set:
            self.machines[i].cpu_use -= inst.app.cpu
            self.machines[j].cpu_use += inst.app.cpu
            self.machines[i].mem_use -= inst.app.mem
            self.machines[j].mem_use += inst.app.mem
            self.machines[i].insts.pop(inst.id)
            self.machines[j].insts[inst.id] = inst
            self.machines[j].insts[inst.id].machine_id = j

        for inst in inst2_set:
            self.machines[i].cpu_use += inst.app.cpu
            self.machines[j].cpu_use -= inst.app.cpu
            self.machines[i].mem_use += inst.app.mem
            self.machines[j].mem_use -= inst.app.mem
            self.machines[j].insts.pop(inst.id)
            self.machines[i].insts[inst.id] = inst
            self.machines[i].insts[inst.id].machine_id = i

        print "after swap: machine1 cpu(%f) mem(%f), machine2 cpu(%f) mem(%f)" % (
            self.machines[i].cpu_score, self.machines[i].mem_score, self.machines[j].cpu_score,
            self.machines[j].mem_score)

    def score_after_swap(self, inst1, inst2):
        machine1 = self.machines[inst1.machine_id]
        machine2 = self.machines[inst2.machine_id]

        cpu1 = machine1.cpu_use + inst2.app.cpu - inst1.app.cpu
        cpu2 = machine2.cpu_use + inst1.app.cpu - inst2.app.cpu

        mem1 = machine1.mem_use + inst2.app.mem - inst1.app.mem
        mem2 = machine2.mem_use + inst1.app.mem - inst2.app.mem

        disk1 = machine1.disk_use + inst2.app.disk - inst1.app.disk
        disk2 = machine2.disk_use + inst1.app.disk - inst2.app.disk

        cpu_radio1 = max(cpu1) / machine1.cpu_capacity
        cpu_radio2 = max(cpu2) / machine2.cpu_capacity
        cpu_delta = -cpu_radio1 ** 2 - cpu_radio2 ** 2 + machine1.cpu_score ** 2 + machine2.cpu_score ** 2

        mem_radio1 = max(mem1) / machine1.mem_capacity
        mem_radio2 = max(mem2) / machine2.mem_capacity
        mem_delta = -mem_radio1 ** 2 - mem_radio2 ** 2 + machine1.mem_score ** 2 + machine2.mem_score ** 2

        if machine1.cpu_score > 1 and machine1.mem_score > 1:
            if cpu_radio2 <= 0.5 and mem_radio2 <= 1 and cpu_delta > 0.01:
                self.do_swap(inst1, inst2)
                return True

        if cpu_radio2 <= 0.5 and mem_radio1 <= 1 and mem_radio2 <= 1 and cpu_delta > 0.01:
            self.do_swap(inst1, inst2)
            return True

        return False

    def do_swap(self, inst1, inst2):
        i = inst1.machine_id
        j = inst2.machine_id

        self.machines[i].cpu_use = self.machines[i].cpu_use + inst2.app.cpu - inst1.app.cpu
        self.machines[j].cpu_use = self.machines[j].cpu_use + inst1.app.cpu - inst2.app.cpu
        self.machines[i].mem_use = self.machines[i].mem_use + inst2.app.mem - inst1.app.mem
        self.machines[j].mem_use = self.machines[j].mem_use + inst1.app.mem - inst2.app.mem
        self.machines[i].disk_use = self.machines[i].disk_use + inst2.app.disk - inst1.app.disk
        self.machines[j].disk_use = self.machines[j].disk_use + inst1.app.disk - inst2.app.disk

        self.machines[i].insts.pop(inst1.id)
        self.machines[j].insts.pop(inst2.id)
        inst1.machine_id = j
        inst2.machine_id = i
        self.machines[i].insts[inst2.id] = inst2
        self.machines[j].insts[inst1.id] = inst1

        # print "after swap: machine1 cpu(%f) mem(%f), machine2 cpu(%f) mem(%f)" % (
        #     self.machines[i].cpu_score, self.machines[i].mem_score, self.machines[j].cpu_score,
        #     self.machines[j].mem_score)

    def output(self):
        self.machines.sort(key=lambda x: x.disk_capacity, reverse=True)
        with open('search', 'w') as f:
            for machine in self.machines:
                if machine.disk_use == 0:
                    continue
                line = "total(%f,%d): {%s} (%s)\n" % (
                    machine.cpu_score, machine.disk_use,
                    ",".join([str(int(i.app.disk)) for i in machine.insts.values()]),
                    ",".join([i.id for i in machine.insts.values()]))
                f.write(line)

    def write_to_csv(self):
        with open('data/submit.csv', 'w') as f:
            for machine in self.machines:
                self.machine_dict[machine.id] = machine
            for machine in self.bak_machines:
                self.bak_machine_dict[machine.id] = machine
            for i in range(len(self.bak_machines)):
                self.bak_machines[i].app_interfers = self.interfer_index

            for machine in self.machines:
                for inst in machine.insts.values():
                    if inst.raw_machine_id != "":
                        self.bak_machine_dict[inst.raw_machine_id].put_inst(inst)

            # while True:
            # print self.interfer_index[('app_6421', 'app_2530')]

            while self.done < 68219:
                self.deploy_stage1(f)

    def deploy_stage1(self, f):
        for machine in self.machine_dict.values():
            for inst in machine.insts.values():
                if self.bak_machine_dict[machine.id].can_put_inst(inst):
                    self.bak_machine_dict[machine.id].put_inst(inst)
                    self.machine_dict[machine.id].take_out(inst)
                    if inst.raw_machine_id != "":
                        self.bak_machine_dict[inst.raw_machine_id].take_out(inst)
                    f.write("%s,%s\n" % (inst.id, machine.id))
                    self.done += 1
                    print self.done
