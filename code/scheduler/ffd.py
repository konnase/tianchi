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
                    if (machine.disk_capacity - machine.disk_use < app.disk):
                        continue
                    # this machine can hold the instance

                    machine.disk_use += app.disk
                    machine.apps.append(app)  # record application whose instance was deployed to this machine
                    submit_result.append((inst.id, machine.id))
                    break
        # for count, machine in enumerate(self.machines):
        #     print machine.id, machine.disk_capacity
        return submit_result
