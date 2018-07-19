import copy

from scheduler.models import read_from_csv


def read_deploy(path):
    deploys = []
    for line in open(path):
       deploys.append(line.strip().split(','))
    return deploys


def judge():
    insts, apps, machines, app_interfers, app_index, machine_index, instance_index = read_from_csv("data")
    deploys = read_deploy("data/submit.csv")

    inst_dic = {}
    app_dic = {}
    machine_dic = {}
    interfer_dic = {}

    for interf in app_interfers:
        interfer_dic[(interf.app_a, interf.app_b)] = interf
    for app in apps:
        app_dic[app.id] = app
    for inst in insts:
        inst.app = app_dic[inst.app_id]
        inst_dic[inst.id] = inst
    for machine in machines:
        machine_dic[machine.id] = machine
        machine_dic[machine.id].app_interfers = interfer_dic

    for inst in inst_dic.values():
        if inst.raw_machine_id != "":
            machine_dic[inst.raw_machine_id].put_inst(inst)

    for i, (inst_id, machine_id) in enumerate(deploys):
        machine = machine_dic[machine_id]
        inst = inst_dic[inst_id]
        # if i == 723:
        #     break
            # app_dic = copy.deepcopy(machine.app_count)

            # has = app_dic[inst.app_id] if inst.app_id in app_dic else 0
            # for app, cnt in app_dic.iteritems():
            #     if (app, inst.app.id) in machine.app_interfers:
            #         if has + 1 > machine.app_interfers[(app, inst.app.id)].num:
            #             print app, inst.app.id

        if machine_dic[machine_id].can_put_inst(inst_dic[inst_id]):
            print i, inst_id, machine_id
            machine_dic[machine_id].put_inst(inst_dic[inst_id])
            if inst.raw_machine_id != "":
                machine_dic[inst.raw_machine_id].take_out(inst)
        else:
            print i, inst_id, machine_id, "break"
            break


if __name__ == '__main__':
    judge()