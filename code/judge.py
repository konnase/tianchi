# coding=utf-8
from scheduler.models import read_from_csv, Machine, Instance
from scheduler.analyse import Analyse
from optparse import OptionParser


def read_deploy(path):
    deploys = []
    for line in open(path):
        deploys.append(line.strip().split(','))
    return deploys


def judge():
    parser = OptionParser()
    parser.add_option("-s", "--submit_csv", default="submit.csv",
                      dest="submit_csv", help="submit file")
    (options, args) = parser.parse_args()

    insts, app, machines, inst_kv, app_kv, machine_kv = read_from_csv("data")

    deploys = read_deploy(options.submit_csv)

    i = 0
    for inst_id, machine_id in deploys:
        i += 1
        inst = inst_kv[inst_id]
        m = machine_kv[machine_id]

        # 仅用于输出信息
        # if not m.can_put_inst(inst, full_cap=True):
        #     print i, inst_id, machine_id, m.out_of_capacity_inst(inst, full_cap=True), \
        #         m.has_conflict_inst(inst), "break"

        m.put_inst(inst)

    final_check(insts, machines)


def final_check(insts, machines):
    Analyse.rating(machines)

    x_cnt = Analyse.print_abnormal_machine_info(machines)
    if x_cnt > 0:
        print "\nError: has_conflict or out_of_full_capacity machines: %d" % (x_cnt)

    undeployed_inst_cnt = len(Instance.get_undeployed_insts(insts))
    if undeployed_inst_cnt > 0:
        print "\nError: undeployed_inst_cnt: %d" % (undeployed_inst_cnt)

    if undeployed_inst_cnt + x_cnt > 0:
        print "\nTotal Score: 1e9"

    print "\nActual Score: ", Machine.total_score(machines)


if __name__ == '__main__':
    judge()
