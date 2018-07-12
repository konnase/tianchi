import numpy as np
import re
import copy


class Knapsack(object):
    def __init__(self, insts, apps, machines, app_interfers):
        self.insts = insts
        self.machines = machines
        self.apps = apps
        self.app_interfers = app_interfers

        self.inst_index = {}
        self.inst_id_index = {}
        self.interfer_index = {}
        self.bins = []

        self.pre_process_machine()
        self.build_index()
        self.read_lower_bound()

        self.done = set()

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
            self.interfer_index[interfer.app_a] = interfer

        self.inst_index = inst_index
        self.inst_id_index = inst_id_index

    def pre_process_machine(self):
        self.machines = sorted(self.machines, key=lambda x: x.disk_capacity, reverse=True)

    def read_lower_bound(self):
        for i, line in enumerate(open("lower_bound")):
            self.machines[i].bins = map(int, re.findall(r'[^{}]+', line)[1].split(','))

    def fit(self):
        for i, machine in enumerate(self.machines):
            if len(machine.bins) == 0:
                continue

            cpu_cap = copy.copy(machine.cpu / 2)
            mem_cap = copy.copy(machine.mem)

            result = []
            bin_candidate = []

            for bin in machine.bins:
                candidates = filter(lambda x: not x.placed, self.inst_index[bin])
                bin_candidate.append(candidates)

            bin_candidate.sort(key=lambda x: max(x, key=lambda y: y.score).score, reverse=True)

            for bin in bin_candidate:
                for item in bin:
                    print item.app.mem
                    # print item.app.disk, "score", item.score
                    if all(item.app.cpu < cpu_cap) and all(item.app.mem < mem_cap):
                        item.machine_id = machine.id
                        item.placed = True

                        cpu_cap -= item.app.cpu
                        mem_cap -= item.app.mem

                        result.append(item)

                        break

            assert (len(result) == len(machine.bins))

            cpu_usage, mem_usage = self.place_score(result, machine)
            print cpu_usage, mem_usage
            assert cpu_usage <= 0.5
            print "bin(%d) placed(%d)" % (len(machine.bins), len(result))
            print "%d: machine(%s) %s %s" % (i, machine.id, machine.bins, ",".join([str(i.score) for i in result]))

    @staticmethod
    def place_score(result, machine):
        size = len(result[0].app.cpu)
        cpu = np.zeros(size)
        mem = np.zeros(size)

        for item in result:
            cpu += item.app.cpu
            mem += item.app.mem

        return np.linalg.norm(cpu / machine.cpu, ord=1) / size, np.linalg.norm(mem / machine.mem, ord=1) / size

    def knapsack(self):
        unplaced = len(self.insts)
        machine_num = 0

        while unplaced > 0:
            self._knapsack(self.machines[machine_num])

            machine_num += 1
            unplaced = len(filter(lambda x: not x.placed, self.insts))

    def _knapsack(self, machine):
        disk_cap = machine.disk_capacity
        dp = np.zeros((int(disk_cap) + 1, 1))
        g = np.zeros((len(self.insts) + 1, int(disk_cap) + 1))

        for i in range(len(self.insts)):
            if self.insts[i].placed:
                continue
            for j in range(int(disk_cap), int(self.insts[i].app.disk) + 1, -1):
                if dp[j - int(self.insts[i].app.disk)][0] + int(self.insts[i].app.disk) > dp[j][0]:
                    dp[j][0] = dp[j - int(self.insts[i].app.disk)][0] + int(self.insts[i].app.disk)
                    g[i][j] = 1

        x = copy.copy(disk_cap)
        bin = []
        for i in range(len(self.insts) - 1, -1, -1):
            if x < 0:
                break
            if g[i][x]:
                x -= self.insts[i].disk
                self.insts[i].placed = True
                bin.append(self.insts[i])
        print "machine(%s): %d, %s" % (machine.id, dp[disk_cap], ",".join([item.id for item in bin]))

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

    def rating(self):
        cpu_overload_cnt = 0
        mem_overload_cnt = 0
        half_cpu_overload_cnt = 0
        interfer_cnt = 0

        total_cnt = 0

        for machine in self.machines:
            app_dict = {}
            cpu = np.array([0.0] * 98)
            mem = np.array([0.0] * 98)
            disk = 0

            if machine.disk_use > 0:
                total_cnt += 1

            for inst in machine.insts.values():
                cpu += inst.app.cpu
                mem += inst.app.mem
                disk += inst.app.disk
                app_dict[inst.app_id] = app_dict[inst.app_id] + 1 if inst.app_id in app_dict else 0

            for app, cnt in app_dict.iteritems():
                if app in self.interfer_index:
                    app_b = self.interfer_index[app].app_b
                    if app_b in app_dict and app_dict[app_b] > self.interfer_index[app].num:
                        interfer_cnt += 1

            if any(cpu > machine.cpu_capacity):
                cpu_overload_cnt += 1
            if any(mem > machine.mem_capacity):
                mem_overload_cnt += 1
            if any(cpu > machine.cpu_capacity / 2):
                half_cpu_overload_cnt += 1

        print "CPU Overload: %f (%d / %d)" % (float(cpu_overload_cnt) / total_cnt, cpu_overload_cnt, total_cnt)
        print "Memory Overload: %f (%d / %d)" % (float(mem_overload_cnt) / total_cnt, mem_overload_cnt, total_cnt)
        print "Half CPU Overload: %f (%d / %d)" % (
            float(half_cpu_overload_cnt) / total_cnt, half_cpu_overload_cnt, total_cnt)
        print "Constraint violate: %d" % interfer_cnt

    def search(self):
        self.machines = filter(lambda x: x.disk_use > 0, self.machines)

        self.machines.sort(key=lambda x: x.cpu_score)

        for i in range(len(self.machines)):
            for inst_id in self.machines[i].insts.keys():
                self.machines[i].insts[inst_id].machine_id = i

        while True:
            if not self.step():
                break

        self.rating()

    def step(self):
        set1 = filter(lambda x: x.cpu_score > 0.5, self.machines)
        set2 = filter(lambda x: x.cpu_score < 0.45, self.machines)

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

    def score_after_swap(self, inst1, inst2):
        machine1 = self.machines[inst1.machine_id]
        machine2 = self.machines[inst2.machine_id]

        cpu1 = machine1.cpu_use + inst2.app.cpu - inst1.app.cpu
        cpu2 = machine2.cpu_use + inst1.app.cpu - inst2.app.cpu

        mem1 = machine1.mem_use + inst2.app.mem - inst1.app.mem
        mem2 = machine2.mem_use + inst1.app.mem - inst2.app.mem

        cpu_radio1 = max(cpu1) / machine1.cpu_capacity
        cpu_radio2 = max(cpu2) / machine2.cpu_capacity
        cpu_delta = -cpu_radio1 ** 2 - cpu_radio2 ** 2 + machine1.cpu_score ** 2 + machine2.cpu_score ** 2

        mem_radio1 = max(mem1) / machine1.mem_capacity
        mem_radio2 = max(mem2) / machine2.mem_capacity
        mem_delta = -mem_radio1 ** 2 - mem_radio2 ** 2 + machine1.mem_score ** 2 + machine2.mem_score ** 2

        if cpu_radio2 <= 0.5 and mem_radio1 <= 1 and mem_radio2 <= 1 and cpu_delta > 0.01:
            # print "after(%.2f,%.2f) before(%.2f,%.2f), delta(%f)" % (
            #     cpu_radio1, cpu_radio2, machine1.cpu_score, machine2.cpu_score, cpu_delta)
            # print "after(%.2f,%.2f) before(%.2f,%.2f), delta(%f)" % (
            #     mem_radio1, mem_radio2, machine1.mem_score, machine2.mem_score, mem_delta)

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

        self.machines[i].insts.pop(inst1.id)
        self.machines[j].insts.pop(inst2.id)
        inst1.machine_id = j
        inst2.machine_id = i
        self.machines[i].insts[inst2.id] = inst2
        self.machines[j].insts[inst1.id] = inst1

        print "after swap: cpu(%f) mem(%f)" % (self.machines[i].cpu_score, self.machines[i].mem_score)

    def output(self):
        self.machines.sort(key=lambda x:x.disk_capacity,reverse=True)
        with open('search', 'w') as f:
            for machine in self.machines:
                line = "total(%f): {%s} (%s)\n" % (machine.cpu_score, ",".join([str(int(i.app.disk)) for i in machine.insts.values()]),
                                               ",".join([i.id for i in machine.insts.values()]))
                f.write(line)