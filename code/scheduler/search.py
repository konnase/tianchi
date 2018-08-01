# coding=utf-8
import random
import numpy as np
from analyse import Analyse
from scheduler.models import Machine, write_to_search


class Search(object):
    def __init__(self, instances, inst_kv, machines):
        self.instances = instances
        self.inst_kv = inst_kv
        self.machines = sorted(
            machines, key=lambda x: x.disk_cap, reverse=True)
        self.total_score = 0
        self.analyse = Analyse(self.instances, self.inst_kv, self.machines)

    def rating(self, input):
        self.analyse.start_analyse(input)
        self._rating()

    def _rating(self):
        self.total_score = self.analyse.rating(self.machines)

    def search(self):
        print "using CPU_UTIL_LARGE: %.2f and CPU_UTIL_SMALL: %.2f" % \
              (self.machines[0].CPU_UTIL_THRESHOLD,
               self.machines[3000].CPU_UTIL_THRESHOLD)

        set1 = range(len(self.machines))
        set2 = range(len(self.machines))
        while True:
            if not self._search(set1, set2):
                break

    # todo: 搜索过程中，占用机器的总数可能会减少，这是正常的
    # 但有可能会丢掉少数几个实例，可能代码中还有bug
    def _search(self, set1, set2):
        random.shuffle(set1) # random.Random(seed).shuffle(set1)
        random.shuffle(set2)
        print '...'

        swap_cnt = 0
        has_change = False
        for i in set1:
            for j in set2:
                if i == j:
                    continue
                # 只取同类app中一个实例迁移或交换
                choice = self.choice()
                if choice == 1:
                    for insts in self.machines[j].app_kv.values():
                        inst = insts[0]
                        if self.try_move_inst(self.machines[i], inst):
                            print "move %s -> %s: %f" % (
                                inst.id, self.machines[i].id, self.total_score)
                            has_change = True
                elif choice == 2:
                    for insts1 in self.machines[i].app_kv.values():
                        for insts2 in self.machines[j].app_kv.values():
                            inst1 = insts1[0]
                            inst2 = insts2[0]
                            if self.try_swap_inst(inst1, inst2):
                                swap_cnt += 1
                                if swap_cnt % 100 == 1:
                                    print "swap %s <-> %s: %f" % (
                                        inst1.id, inst2.id, self.total_score)
                                has_change = True
        return has_change

    def choice(self):
        rand = random.random()
        if rand < 0.4:
            return 1
        else:
            return 2

    def try_move_inst(self, machine1, inst):
        if machine1.disk_usage == 0:
            return False

        if not machine1.can_put_inst(inst, full_cap=True):
            return False

        machine2 = inst.machine

        score_before = machine1.score + machine2.score

        machine1.put_inst(inst)  # put_inst 时会自动将inst从旧的机器移除

        score_after = machine1.score + machine2.score

        if score_after >= score_before:
            machine2.put_inst(inst)  # 恢复原状
            return False

        self.total_score += score_after - score_before
        return True

    def try_swap_inst(self, inst1, inst2):
        # 同一应用的实例就不必交换了
        if inst1.app == inst2.app:
            return False

        machine1 = inst1.machine
        machine2 = inst2.machine

        resource1 = machine1.usage - inst1.app.resource + inst2.app.resource
        resource2 = machine2.usage - inst2.app.resource + inst1.app.resource

        if any(np.around(resource1, 8) > np.around(machine1.full_cap)) or any(
                np.around(resource2, 8) > np.around(machine2.full_cap)):
            return False

        score1 = machine1.score
        score2 = machine2.score

        if self.has_conflict(inst1, inst2) or self.has_conflict(inst2, inst1):
            return False

        self.do_swap(inst1, inst2)

        delta = machine1.score + machine2.score - score1 - score2

        if delta >= 0:
            self.do_swap(inst1, inst2)  # 恢复原状
            return False

        self.total_score += delta
        return True

    # 从机器上移除 inst_old 之后,
    # 检查 inst_new 是否有亲和冲突
    # 注意参数顺序
    def has_conflict(self, inst_old, inst_new):
        machine = inst_old.machine

        machine.remove_inst(inst_old)
        flag = machine.has_conflict_inst(inst_new)
        machine.put_inst(inst_old)  # 恢复原状

        return flag

    def do_swap(self, inst1, inst2):
        machine1 = inst1.machine
        machine2 = inst2.machine
        machine1.put_inst(inst2)
        machine2.put_inst(inst1)

    def output(self):
        self.machines.sort(key=lambda x: x.disk_cap, reverse=True)
        cnt = self.print_abnormal_machine_info(self.machines)
        if cnt > 0: return

        self.analyse.print_undeployed_inst_info(self.instances)

        total_score = Machine.total_score(self.machines)
        machine_cnt = Machine.used_machine_count(self.machines)

        s = ("%.2f_%dm" % (total_score, machine_cnt)).replace(".", "_")
        write_to_search("search-result/search_%s" % (s), self.machines)

    @staticmethod
    def print_abnormal_machine_info(machines):
        out_of_cap_set, conflict_set = Machine.get_abnormal_machines(machines)
        if len(out_of_cap_set) > 0:
            print "Invalid search result: resource overload # %d" % len(out_of_cap_set)
        if len(conflict_set) > 0:
            print "Invalid search result: constraint conflict # %d" % len(conflict_set)
        for m in out_of_cap_set:
            print m.usage > m.capacity
            # print machine.capacity - machine.usage
            print m.id
        for m in conflict_set:
            print m.id, m.get_conflict_list()

        return len(out_of_cap_set) + len(conflict_set)
