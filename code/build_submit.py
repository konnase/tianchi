import datetime
import time

from scheduler.models import read_from_csv
from optparse import OptionParser
from scheduler.models import get_apps_instances, prepare_apps_interfers

submit_result = []


def submit():
    parser = OptionParser()
    parser.add_option("-s", "--search", dest="search", help="input search file")
    (options, args) = parser.parse_args()
    instances, apps, machines, app_interfers, app_index, machine_index, instance_index = read_from_csv("data")
    get_apps_instances(instances, apps, app_index)

    prepare_apps_interfers(app_interfers, app_index, apps)

    machine_dict = {}
    for machine in machines:
        machine_dict[machine.id] = machine

    app_dict = {}
    for app in apps:
        app_dict[app.id] = app

    inst_dict = {}
    for inst in instances:
        inst_dict[inst.id] = inst
        inst.app = app_dict[inst.app_id]
        if inst.machine_id != '':
            inst.machine = machine_dict[inst.machine_id]
            get_init_deploy(inst)  # get init deployment on machine
        app_dict[inst.app_id].instances.append(inst)

    machines.sort(key=lambda x: x.disk_capacity, reverse=True)
    machine_count = 0
    with open(options.search, "r") as f:
        for line in f:

            # get instances' ids of line
            instances_id = line.split()[2].strip('(').strip(')').split(',')
            machine = machines[machine_count]

            # remove all init insts in the machine
            remove_all_inst_in_machine(machine, machines)

            for inst_id in instances_id:
                inst = inst_dict[inst_id]
                if inst.placed:
                    inst.machine.remove_inst(inst)
                machine.put_inst(inst)
                submit_result.append((inst.id, machine.id))
                print "deployed %s to %s" % (inst.id, machine.id)
            machine_count += 1


def remove_all_inst_in_machine(machine, machines):
    for inst in machine.insts.values()[:]:
        for machine_dest in machines:
            if machine_dest.id != machine.id and machine_dest.can_deploy_inst(inst):
                machine.remove_inst(inst)
                machine_dest.put_inst(inst)
                submit_result.append((inst.id, machine_dest.id))
                print "move %s to %s" % (inst.id, machine_dest.id)
                break


def get_init_deploy(inst):
    inst.machine.put_inst(inst)


def write_to_csv():
    z = datetime.datetime.now()
    with open("data/submit.csv", "w") as f:
        for count, item in enumerate(submit_result):
            f.write("{0},{1}\n".format(item[0], item[1]))


def main():
    submit()
    write_to_csv()


if __name__ == '__main__':
    main()
