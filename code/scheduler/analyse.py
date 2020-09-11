# coding=utf-8
from scheduler.models import Instance, Machine, write_to_submit_csv


# 检查 search result
# 是否存在资源超额，亲和冲突，未部署的实例，重复部署的实例
class Analyse(object):
    def __init__(self, instances, inst_kv, machines):
        self.instances = instances
        self.inst_kv = inst_kv
        self.machines = machines
        self.machines.sort(key=lambda x: x.disk_cap, reverse=True)
        Machine.empty(self.machines)

        self.inst_count = 0
        self.inst_cnt_kv = {}
        self.submit_result = []

    def start_analyse(self, search_file):
        machine_index = 0

        for line in open(search_file, "r"):
            instId_list = self.get_instId_list(line)
            m = self.machines[machine_index]

            if 0 == self.deploy_insts(instId_list, m):
                raise Exception('Failed to deploy instances ' + line)

            self.inst_count += len(instId_list)
            machine_index += 1

    def deploy_insts(self, instId_list, machine):
        count = 0
        for inst_id in instId_list:
            if inst_id == '':
                break
            self.inst_cnt_kv.setdefault(inst_id, 0)
            self.inst_cnt_kv[inst_id] += 1
            count += 1

            inst = self.inst_kv[inst_id]
            machine.put_inst(inst)  # 不需要检查约束
            self.submit_result.append((inst_id, machine.id))

        return count

    def print_info(self):
        self.rating(self.machines)
        self.print_multi_deployed_inst_info()
        self.print_undeployed_inst_info(self.instances)
        self.print_abnormal_machine_info(self.machines)

    def print_multi_deployed_inst_info(self):
        # 重构后，部署实例时都会把实例从原机器移除，
        # 应该不会再出现实例多重部署的情况了
        multi_deployed_insts_num = 0
        for count in self.inst_cnt_kv.values():
            if count > 1:
                multi_deployed_insts_num += 1

        if multi_deployed_insts_num > 0:
            print("multi-deployed insts num:%s" % multi_deployed_insts_num)

    @staticmethod
    def print_undeployed_inst_info(instances):
        undeployed_insts = Instance.get_undeployed_insts(instances)
        undeployed_cnt = len(undeployed_insts)
        if undeployed_cnt > 0:
            print("undeployed insts num:%d, for example:" % len(undeployed_insts))
            print(",".join(inst.id for inst in undeployed_insts))

        return undeployed_cnt

    # 资源和亲和约束
    @staticmethod
    def print_abnormal_machine_info(machines):
        out_of_cap_set, conflict_set = Machine.get_abnormal_machines(machines)
        for m in out_of_cap_set:
            print("%s, [%d], %.2f, %.2f/%.2f, %.2f/%.2f, %.0f, %.0f, %.0f, %.0f" % \
                  (m.id, m.disk_cap, m.score,
                   m.cpu_util_avg, m.cpu_util_max,
                   m.mem_util_avg, m.mem_util_max,
                   m.disk_usage,
                   m.pmp_usage[0], m.pmp_usage[1], m.pmp_usage[2]))

        # [(appId_a, appId_b, appCnt_b, limit)]
        for m in conflict_set:
            for x in m.get_conflict_list():
                print("%s, appA:%s, appB:%s, appB deployed:%d, appB limit:%d" % \
                      (m.id, x[0], x[1], x[2], x[3]))
        return len(out_of_cap_set) + len(conflict_set)

    @staticmethod
    def rating(machines):
        total_cnt = 0
        total_score = 0

        disk_overload_cnt = 0
        cpu_overload_cnt = 0
        mem_overload_cnt = 0
        pmp_overload_cnt = 0  # 这三项暂时没有遇到超额的，为了简便，合并显示
        interference_cnt = 0
        violate_cnt = 0

        for m in machines:
            if m.disk_usage == 0:
                continue

            total_cnt += 1
            total_score += m.score

            if m.is_disk_overload:
                disk_overload_cnt += 1
            if m.is_cpu_overload:
                cpu_overload_cnt += 1
            if m.is_mem_overload:
                mem_overload_cnt += 1

            pmp_overload_cnt += m.pmp_overload_cnt
            conflict_list = m.get_conflict_list()
            interference_cnt += len(conflict_list)
            for x in conflict_list:
                # (appId_a, appId_b, appCnt_b, limit)
                violate_cnt += m.app_cnt_kv[x[1]]

        print("CPU Score: %.4f" % total_score)
        print("Disk Overload: %.4f (%d / %d)" % (float(disk_overload_cnt) /
                                                 total_cnt, disk_overload_cnt, total_cnt))
        print("CPU Overload: %.4f (%d / %d)" % (float(cpu_overload_cnt) /
                                                total_cnt, cpu_overload_cnt, total_cnt))
        print("Memory Overload: %.4f (%d / %d)" % (float(mem_overload_cnt) /
                                                   total_cnt, mem_overload_cnt, total_cnt))
        print("PMP Overload %.4f (%d / %d)" % (float(pmp_overload_cnt) /
                                               total_cnt, pmp_overload_cnt, total_cnt))
        print("Constraint violate: %d / %d" % (interference_cnt, violate_cnt))
        return total_score

    @staticmethod
    def get_instId_list(line):
        if line.startswith("undeployed"):
            return []
        # line.split()[2][1:-1].split(',')
        return line.split()[2].strip('(').strip(')').split(',')

    # submit0.csv 不考虑初始的部署，对应 data/b0.csv
    # 因为仅测试用，所以使用固定的名字，会覆盖之前的文件
    def write_to_csv(self):
        write_to_submit_csv("submit0.csv", self.submit_result)
