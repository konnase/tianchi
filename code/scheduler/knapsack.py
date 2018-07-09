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
        self.bins = []

        self.pre_process_machine()
        self.build_index()
        self.read_lower_bound()

    def build_index(self):
        inst_index = {}
        inst_id_index = {}
        self.insts = sorted(self.insts, key=lambda x: x.score, reverse=True)

        for inst in self.insts:
            inst_id_index[inst.id] = inst
            if inst.app.disk not in inst_index:
                inst_index[inst.app.disk] = list()
            inst_index[inst.app.disk].append(inst)

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
        cpu_overload_cnt = 0
        mem_overload_cnt = 0
        half_cpu_overload_cnt = 0
        total_cnt = 0
        undeployed = 0
        for i, line in enumerate(open(output)):
            total_cnt += 1
            if i >= 3000:
                cpu_cap = np.array([32.0] * 98)
                mem_cap = np.array([64.0] * 98)
                disk_cap = 600.0
            else:
                cpu_cap = np.array([92.0] * 98)
                mem_cap = np.array([288.0] * 98)
                disk_cap = 1024.0
            if line.startswith("undeployed"):
                undeployed = len(line.split(','))
                continue

            score = line.split()[0][6:-1][:-1]
            insts = line.split()[2][1:-1].split(',')

            cpu = np.array([0.0] * 98)
            mem = np.array([0.0] * 98)
            disk = 0

            for inst in insts:
                cpu += self.inst_id_index[inst].app.cpu
                mem += self.inst_id_index[inst].app.mem
                disk += self.inst_id_index[inst].app.disk

            overload = False
            cpu_overload = False
            mem_overload = False

            if any(cpu > cpu_cap):
                cpu_overload = True
                cpu_overload_cnt += 1
            if any(mem > mem_cap):
                mem_overload = True
                mem_overload_cnt += 1
            if any(cpu > cpu_cap / 2):
                half_cpu_overload_cnt += 1

            print "%d: %s %f %f %f %s %s" % (
                i, score, (cpu / cpu_cap).sum() / 98, (mem / mem_cap).sum() / 98, disk / disk_cap, cpu_overload, mem_overload)

        print "CPU Overload: %f (%d / %d)" % (float(cpu_overload_cnt) / total_cnt, cpu_overload_cnt, total_cnt)
        print "Memory Overload: %f (%d / %d)" % (float(mem_overload_cnt) / total_cnt, mem_overload_cnt, total_cnt)
        print "Half CPU Overload: %f (%d / %d)" % (float(half_cpu_overload_cnt) / total_cnt, half_cpu_overload_cnt, total_cnt)
        print "Undeployed: %d" % undeployed
