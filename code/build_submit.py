# coding=utf-8
from scheduler.models import read_from_csv, Machine
from optparse import OptionParser

submit_result = []
total_score = 0


def submit():
    parser = OptionParser()
    parser.add_option("-s", "--search", dest="search", help="input search file")
    (options, args) = parser.parse_args()
    insts, apps, machines, inst_kv, app_kv, machine_kv = read_from_csv("data")

    machines.sort(key=lambda x: x.disk_cap, reverse=True)
    machine_count = 0
    for line in open(options.search, "r"):
        if line.startswith("undeployed"):
            continue
        machine = machines[machine_count]
        migrate(machine, machines)

        # get instances' ids of line
        instId_list = line.split()[2].strip('(').strip(')').split(',')
        for inst_id in instId_list:
            inst = inst_kv[inst_id]
            machine.put_inst(inst)
            submit_result.append((inst_id, machine.id))
            # print "deployed %s to %s" % (inst.id, machine.id)
        machine_count += 1
        if machine_count % 500 == 0:
            print machine_count
    global total_score
    total_score = Machine.total_score(machines)
    print "Total score: %.2f on %d machines" % (total_score, machine_count)


def migrate(machine_from, machines):
    for inst in machine_from.inst_kv.values()[:]:
        for machine_to in machines:
            if machine_to != machine_from \
                    and machine_to.can_put_inst(inst, full_cap=True):
                machine_to.put_inst(inst)
                submit_result.append((inst.id, machine_to.id))
                # print "move %s to %s" % (inst.id, machine_to.id)
                break
        else:
            print "Cannot migrate %s from %s" % (inst.id, machine_from.id)


def write_to_csv():
    s = ("%.2f" % total_score).replace(".", "_")
    csv = "submit%s.csv" % s
    print "writing to", csv
    with open(csv, "w") as f:
        for inst_id, machine_id in submit_result:
            f.write("{0},{1}\n".format(inst_id, machine_id))


if __name__ == '__main__':
    submit()
    write_to_csv()
