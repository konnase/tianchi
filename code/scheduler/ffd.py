import models

class FFD(object):
    def __init__(self, instances, applications, machines, app_interfers):
        self.instances = instances
        self.machines = machines
        self.applications = applications
        self.app_interfers = app_interfers

    def fit(self):
        submit_result = []
        apps = self.applications
        apps.sort(key=lambda x:x.disk, reverse=True)
        self.machines.sort(key=lambda x:x.disk_capacity, reverse=True)
        for count, app in enumerate(apps):
            # print app.id, app.disk
            # if count > 1000:
            #     break
            for inst in app.instances:
                for machine in self.machines:
                    if machine.disk_capacity - machine.disk_use < app.disk:
                        continue
                    # this machine can hold the instance in disk view
                    if (machine.cpu - machine.cpu_use < app.cpu).any():
                        continue
                    # this machine can hold the instance in cpu view
                    if (machine.mem - machine.mem_use < app.mem).any():
                        # print machine.mem - machine.mem_use, app.mem
                        continue
                    # this machine can hold the instance in memory view
                    for app_interfer in self.app_interfers:
                        if app.id == app_interfer.app_b:
                            if machine.apps_id.count(app_interfer.app_a) > 0:  # already deployed app_a on machine
                                if app_interfer.num > machine.apps_id.count(app.id):
                                    pass
                                else:
                                    print "%s %s %s %s interfer:%s deployed:%s" % (machine.id, app_interfer.app_a, app_interfer.app_b, inst.id, app_interfer.num, machine.apps_id.count(app.id))
                                    break
                    else:
                        # print machine.cpu_use
                        machine.disk_use += app.disk
                        machine.cpu_use += app.cpu
                        machine.mem_use += app.mem
                        machine.apps.append(app)  # record application whose instance was deployed to this machine
                        machine.apps_id.append(app.id)
                        submit_result.append((inst.id, machine.id))
                        break

        # for count, machine in enumerate(self.machines):
        #     print machine.id, machine.disk_capacity
        return submit_result
