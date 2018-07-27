from scheduler.search import Search
import math
from scheduler.knapsack import Knapsack
from scheduler.models import read_from_csv, get_apps_instances, Method
from optparse import OptionParser
from scheduler.ffd import FFD
from scheduler.analyse import start_analyse
from scheduler.models import prepare_apps_interfers
import datetime
import numpy as np


def main():
    parser = OptionParser()
    parser.add_option("-d", "--data_dir", dest="data_dir", help="directory of csv data")
    parser.add_option("-m", "--method", dest="method", type="int", help="method to solve this prolem")
    parser.add_option("-t", "--test_output", dest="test", help="output to test")
    parser.add_option("-s", "--search", dest="search", help="file to search")
    parser.add_option("-p", "--request", dest="request", help="print request file")
    (options, args) = parser.parse_args()

    insts, apps, machines, app_interfers, app_index, machine_index, instance_index = read_from_csv(options.data_dir)
    get_apps_instances(insts, apps, app_index)

    prepare_apps_interfers(app_interfers, app_index, apps)
    # for app in apps:
    #     print app.id, app.interfer_by_others

    if Method(options.method) == Method.FFD:
        ffd = FFD(insts, apps, machines, app_interfers, machine_index, app_index)
        # ffd algorithm
        ffd.fit()
        with open("init_deploy_conflict.csv", "w") as f:
            for count, item in enumerate(ffd.init_deploy_conflict):
                f.write("{0}\n".format(item))
        with open("submit.csv", "w") as f:
            for count, item in enumerate(ffd.submit_result):
                f.write("{0},{1}\n".format(item[0], item[1]))
        z = datetime.datetime.now()
        final_score = 0
        with open("search-result/search_%s%s_%s%s" % (z.month, z.day, z.hour, z.minute), "w") as f:
            for count, machine in enumerate(machines):
                inst_disk = ""
                inst_id = ""
                all_disk_use = 0
                score = 0
                for i in range(98):
                    score += (1 + 10 * (math.exp(max(0, (machine.cpu_use[i] / machine.cpu_capacity) - 0.5)) - 1))
                for inst in machine.insts.values():
                    inst_disk += "," + str(inst.app.disk)
                    inst_id += "," + str(inst.id)
                    all_disk_use += inst.app.disk
                if all_disk_use == 0:
                    continue
                final_score += score / 98
                f.write("total(%s,%s): {%s}, (%s)\n" % (score / 98, all_disk_use, inst_disk.lstrip(','), inst_id.lstrip(',')))
        print final_score
    elif Method(options.method) == Method.Knapsack:
        knapsack = Knapsack(insts, apps, machines, app_interfers)
        if options.request:
            knapsack.print_request()
        if options.test:
                # knapsack.read_lower_bound()
                knapsack.test(options.test)
                # knapsack.fix_bug()
                # knapsack.output()
                # knapsack.write_to_csv()
        if options.search:
            try:
                # knapsack.read_lower_bound()
                knapsack.test(options.search)
                knapsack.search()
                knapsack.output()
            except KeyboardInterrupt:
                print "write to file."
                knapsack.output()
    elif Method(options.method) == Method.Analyse:
        search_file = options.search
        ffd = FFD(insts, apps, machines, app_interfers, machine_index, app_index)
        results = start_analyse(instance_index, ffd, search_file)
        with open("analyse.csv", "w") as f:
            for line in results:
                f.write(line)
        with open("knapsack_deploy_conflict.csv", "w") as f:
            for count, item in enumerate(ffd.init_deploy_conflict):
                f.write("{0}\n".format(item))
    elif Method(options.method) == Method.Search:
        search = Search(insts, apps, machines, app_interfers)
        search.rating(options.search)
        try:
            search.search()
        except KeyboardInterrupt:
            print "write to file."
            search.output()


if __name__ == '__main__':
    main()
