# coding=utf-8
import numpy as np


# 检查 search result
class Analyse(object):
    def __init__(self, instance_kv, machines, instances):
        self.instance_kv = instance_kv
        self.machines = machines
        self.instances = instances

        self.init_deploy_conflict = []
        self.results = []
        self.single_inst_count = {}
        self.is_inst_deployed = {}

        self.machines.sort(key=lambda x: x.disk_cap, reverse=True)
        self.max_cpu_use = 0.0
        self.avg_cpu_use = 0.0
        self.max_mem_use = 0.0
        self.avg_mem_use = 0.0

    def start_analyse(self, search_file):
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
                insts_id = self.get_inst_id_list(line)
                # print instances_id
                m = self.machines[machine_count]
                m.clear_instances()
                inst_count = self.deploy_inst(insts_id, m)
                if inst_count == 0:
                    continue
                out_of_capacity = m.out_of_capacity()
                self.avg_cpu_use /= inst_count
                self.avg_mem_use /= inst_count
                self.append_result(m, out_of_capacity)

                final_score += m.score
                all_inst += len(insts_id)
                machine_count += 1
        self.print_watch_message(final_score, all_inst)
        self.resolve_init_conflict(self.machines[0:machine_count])

    def deploy_inst(self, insts_id, machine):
        inst_count = 0
        for inst_id in insts_id:
            if inst_id == '':
                break
            self.single_inst_count.setdefault(inst_id, 0)
            self.single_inst_count[inst_id] += 1

            inst_count += 1
            index = self.instance_kv[inst_id]
            machine.put_inst(self.instances[index])  # todo: 需要检查约束吗??
            if np.max(self.instances[index].app.cpu) > self.max_cpu_use:
                self.max_cpu_use = np.max(self.instances[index].app.cpu)
            if np.max(self.instances[index].app.mem) > self.max_mem_use:
                self.max_mem_use = np.max(self.instances[index].app.mem)
            self.avg_cpu_use += np.average(self.instances[index].app.cpu)
            self.avg_mem_use += np.average(self.instances[index].app.mem)
        return inst_count

    def resolve_init_conflict(self, machines):
        print "starting resolve_init_conflict"
        # todo: ???

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
        for inst in self.instances:
            if inst.id not in self.single_inst_count.keys():
                print inst.id
                break

    @staticmethod
    def get_inst_id_list(line):
        return line.split()[2].strip('(').strip(')').split(',')

    def write_to_csv(self):
        with open("analyse.csv", "w") as f:
            for line in self.results:
                f.write(line)
        with open("knapsack_deploy_conflict.csv", "w") as f:
            for count, item in enumerate(self.deploy_conflict):
                f.write("{0}\n".format(item))
