# coding=utf-8
from scheduler.search import Search
from scheduler.knapsack import Knapsack
from optparse import OptionParser
from scheduler.ffd import FFD
from scheduler.analyse import Analyse
from scheduler.models import read_from_csv, AppInterference
import scheduler.constant as config

from enum import Enum


class Method(Enum):
    FFD = 1
    Knapsack = 2
    Analyse = 3
    If_Search_Has_Init_Conflict = 4
    Search = 5


def main():
    parser = OptionParser()
    parser.add_option("-d", "--data_dir", dest="data_dir", help="directory of csv data")
    parser.add_option("-m", "--method", dest="method", type="int", help="method to solve this problem")
    parser.add_option("-t", "--test_output", dest="test", help="output to test")
    parser.add_option("-s", "--search", dest="search", help="file to search")
    parser.add_option("-p", "--request", dest="request", help="print request file")
    parser.add_option("--larger_cpu_util", dest="larger_cpu_util", default=1, type="float",
                      help="larger machine's maximal cpu utilization")
    parser.add_option("--smaller_cpu_util", dest="smaller_cpu_util", default=1, type="float",
                      help="smaller machine's maximal cpu utilization")
    (options, args) = parser.parse_args()

    # 需要在读入 machine 数据之前就修改这两个参数
    config.CPU_UTIL_LARGE = options.larger_cpu_util
    config.CPU_UTIL_SMALL = options.smaller_cpu_util

    insts, apps, machines, insts_kv, app_kv, machine_kv = read_from_csv(options.data_dir)

    if Method(options.method) == Method.FFD:
        ffd = FFD(insts, apps, machines, app_kv, machine_kv)
        ffd.fit()
        ffd.write_to_csv()

    elif Method(options.method) == Method.Knapsack:
        knapsack = Knapsack(insts, apps, machines, AppInterference)
        if options.request:
            knapsack.print_request()
        if options.test:
            knapsack.read_lower_bound()
            knapsack.test(options.test)
            # knapsack.fix_bug()
            # knapsack.output()
            knapsack.write_to_csv()
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
        analyse = Analyse(insts_kv, machines, insts)
        analyse.start_analyse(search_file)
        analyse.write_to_csv()

    elif Method(options.method) == Method.Search:
        search = Search(insts, apps, machines, AppInterference)
        search.rating(options.search)
        try:
            search.search()
        except KeyboardInterrupt:
            print "write to file."
            search.output()


if __name__ == '__main__':
    main()
