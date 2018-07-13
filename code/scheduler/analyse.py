import numpy as np

def start_analyse(insts, apps, machines, app_interfers, machine_index):

    inputfile = []
    with open("search", "r") as f:
        for line in f:
            instances_id = line.split()[2].strip('(').strip(')').split(',')
            print instances_id

            out_of_capacity = False
            max_cpu_use = 0.0
            avg_cpu_use = 0.0
            max_mem_use = 0.0
            avg_mem_use = 0.0
            count = 0

            for inst_id in instances_id:
                ++count
                for inst in insts:
                    if inst_id == inst.id:
                        if np.max(inst.app.cpu) > max_cpu_use:
                            max_cpu_use = np.max(inst.app.cpu)
                        if np.max(inst.app.mem) > max_mem_use:
                            max_mem_use = np.max(inst.app.mem)
                        avg_cpu_use += np.average(inst.app.cpu)
                        avg_mem_use += np.average(inst.app.mem)

            avg_cpu_use /= count
            avg_mem_use /= count
