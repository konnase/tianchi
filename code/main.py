from scheduler.models import read_from_csv
from optparse import OptionParser


def main():
    parser = OptionParser()
    parser.add_option("-d", "--data_dir", dest="data_dir", help="directory of csv data")
    (options, args) = parser.parse_args()

    instances, applications, machines = read_from_csv(options.data_dir)
    print len(instances), len(applications), len(machines)

if __name__ == '__main__':
    main()
