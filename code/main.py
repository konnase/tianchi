from scheduler.models import read_from_csv
from scheduler.models import get_apps_instances
from optparse import OptionParser
from scheduler.ffd import FFD


def main():
    parser = OptionParser()
    parser.add_option("-d", "--data_dir", dest="data_dir", help="directory of csv data")
    (options, args) = parser.parse_args()

    instances, applications, machines, app_interfers = read_from_csv(options.data_dir)
    get_apps_instances(instances, applications)
    print len(instances), len(applications), len(machines), len(app_interfers)

    # for app in applications:
    #     print app.instances
    ffd = FFD(instances, applications, machines, app_interfers)
    ffd.fit()
    f = open("machine_tasks.txt", "w")
    for count, machine in enumerate(machines):
        inst_disk = []
        for app in machine.apps:
            inst_disk.append(app.disk)
        f.write("number {0}, total({1}), ({2})\n".format(count, machine.disk_capacity, inst_disk))
    f.close()

if __name__ == '__main__':
    main()
