from scheduler.models import read_from_csv, Machine, AppInterference


def read_deploy(path):
    deploys = []
    for line in open(path):
        deploys.append(line.strip().split(','))
    return deploys


def judge():
    insts, app, machines, inst_kv, app_kv, machine_kv = read_from_csv("data")

    deploys = read_deploy("submit.csv")

    i = 0
    for inst_id, machine_id in deploys:
        i += 1
        m = machine_kv[machine_id]
        inst = inst_kv[inst_id]

        if m.can_put_inst(inst):
            m.put_inst(inst)
        else:
            print i, inst_id, machine_id, "break"

    print "Total Score: ", Machine.total_score(machines)


if __name__ == '__main__':
    judge()
