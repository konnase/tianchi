from scheduler.knapsack import Knapsack
from scheduler.models import read_from_csv, get_apps_instances, Method
from optparse import OptionParser
from scheduler.ffd import FFD


def main():
    parser = OptionParser()
    parser.add_option("-d", "--data_dir", dest="data_dir", help="directory of csv data")
    parser.add_option("-m", "--method", dest="method", type="int", help="method to solve this prolem")
    parser.add_option("-t", "--test_output", dest="test", help="output to test")
    parser.add_option("-s", "--search", dest="search", help="file to search")
    (options, args) = parser.parse_args()

    insts, apps, machines, app_interfers, app_index = read_from_csv(options.data_dir)
    get_apps_instances(insts, apps, app_index)

    if Method(options.method) == Method.FFD:
        ffd = FFD(insts, apps, machines, app_interfers)
        ffd.fit()
        with open("machine_tasks.txt", "w") as f:
            for count, machine in enumerate(machines):
                inst_disk = []
                for app in machine.apps:
                    inst_disk.append(app.disk)
                f.write("number {0}, total({1}), ({2})\n".format(count, machine.disk_capacity, inst_disk))
    elif Method(options.method) == Method.Knapsack:
        knapsack = Knapsack(insts, apps, machines, app_interfers)
        if options.test:
                knapsack.test(options.test)
        if options.search:
            try:
                knapsack.test(options.search)
                knapsack.search()
                knapsack.output()
            except KeyboardInterrupt:
                print "write to file."
                knapsack.output()

# for count, machine in enumerate(machines):
    #     print machine.cpu_capacity, machine.cpu, machine.cpu_use
    #     if count > 20:
    #         break
    # for app in applications:
    #     print app.instances

if __name__ == '__main__':
    main()
