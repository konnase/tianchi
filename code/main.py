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

    # for count, machine in enumerate(machines):
    #     print machine.cpu_capacity, machine.cpu, machine.cpu_use
    #     if count > 20:
    #         break
    # for app in applications:
    #     print app.instances
    ffd = FFD(instances, applications, machines, app_interfers)
    submit = ffd.fit()
    with open("submit.csv", "w") as f:
        for count, item in enumerate(submit):
            f.write("{0} {1}\n".format(item[0], item[1]))
    with open("machine_tasks.txt", "w") as f:
        for count, machine in enumerate(machines):
            inst_disk = []
            for app in machine.apps:
                inst_disk.append(app.disk)
            f.write("number {0}, total({1}), ({2})\n".format(count, machine.disk_capacity, inst_disk))


if __name__ == '__main__':
    main()
