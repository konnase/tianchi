import math
import time

import numpy as np

from constant import *


class Analyse(object):
    def __init__(self, instance_index, machines, instances):
        self.instance_index = instance_index
        self.machines = machines
        self.instances = instances

        self.init_deploy_conflict = []
        self.results = []
        self.single_inst_count = {}
        self.is_inst_deployed = {}

        self.machines.sort(key=lambda x: x.disk_capacity, reverse=True)
        self.larger_disk_capacity = self.machines[0].disk_capacity
        self.smaller_disk_capacity = self.machines[5999].disk_capacity
        self.larger_cpu_util = 0.0
        self.smaller_cpu_util = 0.0
        self.max_cpu_use = 0.0
        self.avg_cpu_use = 0.0
        self.max_mem_use = 0.0
        self.avg_mem_use = 0.0
        self.cpu_overload = 0
        self.mem_overload = 0
        self.p_overload = 0
        self.m_overload = 0
        self.pm_overload = 0

    def start_analyse(self, search_file, larger_cpu_util, smaller_cpu_util):
        self.larger_cpu_util = larger_cpu_util
        self.smaller_cpu_util = smaller_cpu_util
        machine_count = 0
        final_score = 0
        all_inst = 0
        self.results.append("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
            'out_of_capacity', 'max_cpu_use', 'avg_cpu_use', 'max_mem_use', 'avg_mem_use', 'p', 'm', 'pm'))
        with open(search_file, "r") as f:
            for line in f:
                self.max_cpu_use = 0.0
                self.avg_cpu_use = 0.0
                self.max_mem_use = 0.0
                self.avg_mem_use = 0.0
                instances_id = self.get_instance_id(line)
                machine = self.machines[machine_count]
                machine.clean_machine_status()
                inst_count = self.deploy_inst(instances_id, machine)
                if inst_count == 0:
                    continue
                self.record_overload_machine(machine)
                # cpu, mem, p, m, pm = machine.out_of_capacity(self.larger_cpu_util, self.smaller_cpu_util,
                #                                              self.larger_disk_capacity, self.smaller_disk_capacity)
                self.avg_cpu_use /= inst_count
                self.avg_mem_use /= inst_count
                self.append_result(machine, out_of_capacity)
                final_score += self.get_final_score(machine)
                all_inst += len(instances_id)
                machine_count += 1
        self.print_watch_message(final_score, all_inst)
        self.resolve_init_conflict(self.machines[0:machine_count])

    def record_overload_machine(self, machine):
        cpu, mem, p, m, pm = machine.out_of_capacity(self.larger_cpu_util, self.smaller_cpu_util,
                                                     self.larger_disk_capacity, self.smaller_disk_capacity)
        self.cpu_overload += cpu
        self.mem_overload += mem
        self.p_overload += p
        self.m_overload += m
        self.pm_overload += pm

    def deploy_inst(self, instances_id, machine):
        inst_count = 0
        for inst_id in instances_id:
            if inst_id == '':
                break
            if self.single_inst_count.has_key(inst_id):
                self.single_inst_count[inst_id] += 1
            else:
                self.single_inst_count[inst_id] = 1
            inst_count += 1
            index = self.instance_index[inst_id]
            machine.put_inst(self.instances[index])
            if np.max(self.instances[index].app.cpu) > self.max_cpu_use:
                self.max_cpu_use = np.max(self.instances[index].app.cpu)
            if np.max(self.instances[index].app.mem) > self.max_mem_use:
                self.max_mem_use = np.max(self.instances[index].app.mem)
            self.avg_cpu_use += np.average(self.instances[index].app.cpu)
            self.avg_mem_use += np.average(self.instances[index].app.mem)
        return inst_count

    @staticmethod
    def get_final_score(machine):
        score = 0
        for i in range(LINE_SIZE):
            score += (1 + 10 * (math.exp(
                max(0, (machine.cpu_use[i] / machine.cpu_capacity) - 0.5)) - 1))
        return score / LINE_SIZE

    def resolve_init_conflict(self, machines):
        print "starting resolve_init_conflict"
        time.sleep(2)

        for machine in machines:
            for inst in machine.insts.values():
                for inst_b in machine.insts.values():
                    if machine.apps_id.count(inst.app.id) <= 0:
                        break
                    if machine.has_init_conflict(inst, inst_b):
                        self.record_init_conflict(machine, inst, inst_b)
                for inst_interferd in machine.insts.values():
                    if not machine.insts.has_key(inst):
                        break
                    if machine.has_init_conflict(inst_interferd, inst):
                        self.record_init_conflict(machine, inst_interferd, inst)

    def record_init_conflict(self, machine, inst, inst_b):
        self.init_deploy_conflict.append("%s, appA:%s %s, appB:%s %s, interfer:%d, deployed:%d" %
                                         (machine.id, inst.app.id, inst.id, inst_b.app.id, inst_b.id,
                                          inst.app.interfer_others[inst_b.app.id],
                                          machine.apps_id.count(inst_b.app.id)))
        print "%s, appA:%s, appB:%s, interfer:%d, deployed:%d" % \
              (machine.id, inst.app.id, inst_b.app.id, inst.app.interfer_others[inst_b.app.id],
               machine.apps_id.count(inst_b.app.id))

    def append_result(self, machine, out_of_capacity):
        result = "%s\t\t\t%4.3f\t\t%4.3f\t\t%4.3f\t\t%4.3f\t\t%4.3f\t\t%4.3f\t\t%4.3f\n" % (
            out_of_capacity, self.max_cpu_use, self.avg_cpu_use, self.max_mem_use, self.avg_mem_use, machine.p_num,
            machine.m_num, machine.pm_num)
        self.results.append(result)

    def print_watch_message(self, final_score, all_inst):
        print "score:%4.3f,insts_num:%d" % (final_score, all_inst)
        print "Insts below are multi-deployed:"
        multi_deployed_insts_num = 0
        for inst, count in self.single_inst_count.items():
            if count > 1:
                multi_deployed_insts_num += 1
                # print inst, count
        print "multi-deployed insts num:%s" % multi_deployed_insts_num

        print "undeployed insts num:%d, for example:" % (len(self.instances) - len(self.single_inst_count)),
        if (len(self.instances) - len(self.single_inst_count)) == 0:
            print 'None'
            return
        for inst in self.instances:
            if inst.id not in self.single_inst_count.keys():
                print inst.id
                break

    @staticmethod
    def get_instance_id(line):
        return line.split()[2].strip('(').strip(')').split(',')

    def write_to_csv(self):
        print "write resource analysis to analyse.csv..."
        with open("analyse.csv", "w") as f:
            for line in self.results:
                f.write(line)
        print "write interferences analysis to knapsack_deploy_conflict.csv..."
        with open("knapsack_deploy_conflict.csv", "w") as f:
            for count, item in enumerate(self.init_deploy_conflict):
                f.write("{0}\n".format(item))
