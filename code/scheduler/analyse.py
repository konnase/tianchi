import numpy as np


def start_analyse(insts, instance_index):

    results = ["Overload\t\tMaxCpu\t\tAvgCpu\t\tMaxMem\t\tAvgMem\n"]
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
                count += 1
                index = instance_index[inst_id]
                if np.max(insts[index].app.cpu) > max_cpu_use:
                    max_cpu_use = np.max(insts[index].app.cpu)
                if np.max(insts[index].app.mem) > max_mem_use:
                    max_mem_use = np.max(insts[index].app.mem)
                avg_cpu_use += np.average(insts[index].app.cpu)
                avg_mem_use += np.average(insts[index].app.mem)
            if count <= 3000:
                if avg_cpu_use > 46 or avg_mem_use > 288:
                    out_of_capacity = True
            else:
                if avg_cpu_use > 16 or avg_mem_use > 64:
                    out_of_capacity = True
            avg_cpu_use /= count
            avg_mem_use /= count
            result = "%s\t\t%4.3f\t\t%4.3f\t\t%4.3f\t\t%4.3f\n" % (out_of_capacity, max_cpu_use, avg_cpu_use, max_mem_use, avg_mem_use)
            results.append(result)
    return results
