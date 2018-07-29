# coding=utf-8

# 检查 search result
# 是否存在资源超额，亲和冲突，未部署的实例，重复部署的实例
class Analyse(object):
    def __init__(self, inst_kv, machines):
        self.inst_kv = inst_kv
        self.machines = machines
        self.machines.sort(key=lambda x: x.disk_cap, reverse=True)

        self.inst_cnt_kv = {}

    def start_analyse(self, search_file):
        machine_index = 0
        total_score = 0
        inst_count = 0

        for line in open(search_file, "r"):
            insts_id = self.get_instId_list(line)
            m = self.machines[machine_index]
            m.clear_instances()  # 清空初始部署
            if 0 == self.deploy_insts(insts_id, m):
                raise Exception('Failed to deploy instances ' + line)

            total_score += m.score
            inst_count += len(insts_id)
            machine_index += 1

        print "score:%4.3f,insts_num:%d of [%d]" % \
              (total_score, inst_count, len(self.inst_kv))

        self.print_info()

    def deploy_insts(self, insts_id, machine):
        count = 0
        for inst_id in insts_id:
            if inst_id == '':
                break
            self.inst_cnt_kv.setdefault(inst_id, 0)
            self.inst_cnt_kv[inst_id] += 1
            count += 1

            inst = self.inst_kv[inst_id]
            machine.put_inst(inst)  # todo: 没有检查约束

        return count

    def print_info(self):
        multi_deployed_insts_num = 0
        for inst, count in self.inst_cnt_kv.items():
            if count > 1:
                multi_deployed_insts_num += 1

        if multi_deployed_insts_num > 0:
            print "multi-deployed insts num:%s" % multi_deployed_insts_num

        undeployed_inst_num = len(self.inst_kv) - len(self.inst_cnt_kv)
        if undeployed_inst_num > 0:
            print "undeployed insts num:%d, for example:" % undeployed_inst_num
            for inst_id in self.inst_kv.keys():
                if inst_id not in self.inst_cnt_kv.keys():
                    print inst_id
                    break
        self.print_conflict()

    def print_conflict(self):
        for m in self.machines:
            if m.out_of_full_capacity():
                print "%s, %.2f, %.2f / %.2f, %.2f / %.2f, %.0f / %1.0f, %s" % \
                      (m.id, m.score,
                       m.cpu_util_avg, m.cpu_util_max,
                       m.mem_util_avg, m.mem_util_max,
                       m.disk_usage, m.disk_cap, m.pmp_usage)

            list = m.get_conflict_list()
            if len(list) == 0:
                continue
            for x in list:
                print "%s, appA:%s, appB:%s,deployed:%d, limit:%d\n" % \
                      (m.id, x[0], x[1], x[2], x[3])

    @staticmethod
    def get_instId_list(line):
        return line.split()[2].strip('(').strip(')').split(',')
