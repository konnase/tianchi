# coding=utf-8
import datetime
from config import *


class FFD(object):
    def __init__(self, instances, apps, machines, app_kv, machine_kv):
        self.instances = instances
        self.apps = apps
        self.machines = machines
        self.app_index = app_kv
        self.machine_index = machine_kv

        self.submit_result = []

        self.apps.sort(key=lambda x: x.disk, reverse=True)
        self.machines.sort(key=lambda x: x.disk_cap, reverse=True)

    def first_fit(self, inst, machines):
        for m in machines:
            if m.can_put_inst(inst):
                m.put_inst(inst)
                self.submit_result.append((inst.id, m.id))
                break
        else:
            raise Exception("%s is not deployed" % inst)

    def resolve_init_conflict(self, machines):
        print "starting resolve_init_conflict"
        for inst in self.instances:
            # 初始部署存在冲突的实例
            if not inst.deployed and inst.machine is not None:
                self.first_fit(inst, machines)

    def fit_large_inst(self, machines):
        print "starting fit_large_inst"
        for app in self.apps:
            if app.disk < LARGE_DISK_INST \
                    and all(app.cpu < LARGE_CPU_INST) \
                    and all(app.mem < LARGE_MEM_INST):
                continue

            for inst in app.instances:
                if not inst.deployed:
                    self.first_fit(inst, machines)

    def fit(self):
        self.resolve_init_conflict(self.machines)
        self.fit_large_inst(self.machines)
        print "starting fit"
        for app in self.apps:
            for inst in app.instances:
                if not inst.deployed:
                    self.first_fit(inst, self.machines)

    def write_to_csv(self):
        with open("submit.csv", "w") as f:
            for inst_id, machine_id in self.submit_result:
                f.write("{0},{1}\n".format(inst_id, machine_id))

        total_score = 0
        with open("search-result/search_%s" % datetime.datetime.now().strftime('%Y%m%d_%H%M%S'), "w") as f:
            for m in self.machines:
                if len(m.inst_kv) == 0:
                    continue

                inst_disk = ""
                inst_id = ""
                all_disk_use = 0
                score = m.score
                total_score += score

                for inst in m.inst_kv.values():
                    inst_disk += "," + str(inst.app.disk)
                    inst_id += "," + str(inst.id)
                    all_disk_use += inst.app.disk

                f.write("total(%s,%s): {%s}, (%s)\n" % (
                    score, all_disk_use, inst_disk.lstrip(','), inst_id.lstrip(',')))

        print total_score
