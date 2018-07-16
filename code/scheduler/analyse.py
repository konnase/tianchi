import numpy as np

LINESIZE = 98
SEARCH_FILE = "machine_tasks.txt"

def start_analyse(insts, instance_index):

    results = []
    machine_count = 0
    with open(SEARCH_FILE, "r") as f:
        for line in f:
            instances_id = line.split()[2].strip('(').strip(')').split(',')
            print instances_id
            out_of_capacity = False
            max_cpu_use = 0.0
            avg_cpu_use = 0.0
            time_cpu_use = np.zeros(LINESIZE)
            max_mem_use = 0.0
            avg_mem_use = 0.0
            time_mem_use = np.zeros(LINESIZE)
            count = 0

            machine_count += 1
            for inst_id in instances_id:
                if inst_id == '':
                    break
                count += 1
                index = instance_index[inst_id]
                time_cpu_use += insts[index].app.cpu
                time_mem_use += insts[index].app.mem
                if np.max(insts[index].app.cpu) > max_cpu_use:
                    max_cpu_use = np.max(insts[index].app.cpu)
                if np.max(insts[index].app.mem) > max_mem_use:
                    max_mem_use = np.max(insts[index].app.mem)
                avg_cpu_use += np.average(insts[index].app.cpu)
                avg_mem_use += np.average(insts[index].app.mem)
            if machine_count <= 3000:
                if avg_cpu_use > 46 or (time_cpu_use > np.full(LINESIZE, 46)).any() or avg_mem_use > 288 or (time_mem_use > np.full(LINESIZE, 288)).any():
                    out_of_capacity = True
            else:
                # print time_cpu_use
                if avg_cpu_use > 16 or (time_cpu_use > np.full(LINESIZE, 16)).any() or avg_mem_use > 64 or (time_mem_use > np.full(LINESIZE, 64)).any():
                    out_of_capacity = True
            if count == 0:
                continue
            avg_cpu_use /= count
            avg_mem_use /= count
            result = "%s\t\t%4.3f\t\t%4.3f\t\t%4.3f\t\t%4.3f\n" % (out_of_capacity, max_cpu_use, avg_cpu_use, max_mem_use, avg_mem_use)
            results.append(result)
    return results
