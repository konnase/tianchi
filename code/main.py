from scheduler.knapsack import Knapsack
from scheduler.models import read_from_csv, get_apps_instances, Method
from optparse import OptionParser
from scheduler.ffd import FFD


def main():
    parser = OptionParser()
    parser.add_option("-d", "--data_dir", dest="data_dir", help="directory of csv data")
    parser.add_option("-m", "--method", dest="method", type="int", help="method to solve this prolem")
    parser.add_option("-t", "--test_output", dest="test", help="output to test")
    (options, args) = parser.parse_args()

    insts, apps, machines, app_interfers, app_index, machine_index = read_from_csv(options.data_dir)
    get_apps_instances(insts, apps, app_index)

    if Method(options.method) == Method.FFD:
        ffd = FFD(insts, apps, machines, app_interfers, machine_index)
        ffd.fit_before()
        ffd.fit()
        with open("submit.csv", "w") as f:
            for count, item in enumerate(ffd.submit_result):
                f.write("{0},{1}\n".format(item[0], item[1]))
        with open("machine_tasks.txt", "w") as f:
            for count, machine in enumerate(machines):
                inst_disk = []
                inst_id = []
                for inst in machine.insts:
                    inst_disk.append(inst.app.disk)
                    inst_id.append(inst.id)
                f.write("{0}, total({1}), ({2}), ({3})\n".format(machine.id, machine.disk_capacity, inst_disk, inst_id))
        with open("init_deploy_conflict.csv", "w") as f:
            for count, item in enumerate(ffd.init_deploy_conflict):
                f.write("{0}\n".format(item))
    elif Method(options.method) == Method.Knapsack:
        knapsack = Knapsack(insts, apps, machines, app_interfers)
        if options.test:
            knapsack.test(options.test)

    # for count, machine in enumerate(machines):
    #     print machine.cpu_capacity, machine.cpu, machine.cpu_use
    #     if count > 20:
    #         break
    # for app in applications:
    #     print app.instances

if __name__ == '__main__':
    main()
