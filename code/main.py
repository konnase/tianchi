
from scheduler.search import Search

from scheduler.knapsack import Knapsack
from scheduler.models import read_from_csv, get_apps_instances, Method
from optparse import OptionParser
from scheduler.ffd import FFD
from scheduler.analyse import Analyse
from scheduler.models import prepare_apps_interfers
import scheduler.constant as constant


def main():
    parser = OptionParser()
    parser.add_option("-d", "--data_dir", dest="data_dir", help="directory of csv data")
    parser.add_option("-m", "--method", dest="method", type="int", help="method to solve this prolem")
    parser.add_option("-t", "--test_output", dest="test", help="output to test")
    parser.add_option("-s", "--search", dest="search", help="file to search")
    parser.add_option("-p", "--request", dest="request", help="print request file")
    parser.add_option("--larger_cpu_util", dest="larger_cpu_util", default=1, type="float",
                      help="specify larger machine's maximal cpu utilization")
    parser.add_option("--smaller_cpu_util", dest="smaller_cpu_util", default=1, type="float",
                      help="specify smaller machine's maximal cpu utilization")
    (options, args) = parser.parse_args()

    constant.set_cpu_util_large(options.larger_cpu_util)
    constant.set_cpu_util_small(options.smaller_cpu_util)

    insts, apps, machines, app_interfers, app_index, machine_index, instance_index = read_from_csv(options.data_dir)
    get_apps_instances(insts, apps, app_index)

    prepare_apps_interfers(app_interfers, app_index, apps)

    if Method(options.method) == Method.FFD:
        ffd = FFD(insts, apps, machines, app_interfers, machine_index, app_index)
        ffd.fit()
        ffd.write_to_csv()
    elif Method(options.method) == Method.Knapsack:
        knapsack = Knapsack(insts, apps, machines, app_interfers)
        if options.request:
            knapsack.print_request()
        if options.test:
            knapsack.test(options.test)
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
        analyse = Analyse(instance_index, machines, insts)
        larger_cpu_util = options.larger_cpu_util
        smaller_cpu_util = options.smaller_cpu_util
        analyse.start_analyse(search_file, larger_cpu_util, smaller_cpu_util)
        analyse.write_to_csv()

    elif Method(options.method) == Method.Search:
        search = Search(insts, apps, machines, app_interfers)
        search.rating(options.search)
        try:
            search.search()
        except KeyboardInterrupt:
            search._rating()
            search.output()


if __name__ == '__main__':
    main()
