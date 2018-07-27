import math

import numpy as np

from constant import *


def start_analyse(instance_index, ffd, SEARCH_FILE):
    ffd.machines.sort(key=lambda x: x.disk_capacity, reverse=True)
    results = []
    machine_count = 0
    final_score = 0
    with open(SEARCH_FILE, "r") as f:
        for line in f:
            instances_id = line.split()[2].strip('(').strip(')').split(',')
            # print instances_id
            ffd.machines[machine_count].apps_id[:] = []
            ffd.machines[machine_count].mem_use = np.zeros(int(LINE_SIZE))
            ffd.machines[machine_count].cpu_use = np.zeros(int(LINE_SIZE))
            ffd.machines[machine_count].disk_use = 0
            ffd.machines[machine_count].p_num = 0
            ffd.machines[machine_count].m_num = 0
            ffd.machines[machine_count].pm_num = 0
            ffd.machines[machine_count].insts.clear()
            out_of_capacity = False
            max_cpu_use = 0.0
            avg_cpu_use = 0.0
            time_cpu_use = np.zeros(LINE_SIZE)
            max_mem_use = 0.0
            avg_mem_use = 0.0
            time_mem_use = np.zeros(LINE_SIZE)
            inst_count = 0

            for inst_id in instances_id:
                if inst_id == '':
                    break
                inst_count += 1
                index = instance_index[inst_id]
                ffd.machines[machine_count].put_inst(ffd.instances[index])
                time_cpu_use += ffd.instances[index].app.cpu
                time_mem_use += ffd.instances[index].app.mem
                if np.max(ffd.instances[index].app.cpu) > max_cpu_use:
                    max_cpu_use = np.max(ffd.instances[index].app.cpu)
                if np.max(ffd.instances[index].app.mem) > max_mem_use:
                    max_mem_use = np.max(ffd.instances[index].app.mem)
                avg_cpu_use += np.average(ffd.instances[index].app.cpu)
                avg_mem_use += np.average(ffd.instances[index].app.mem)
            if avg_cpu_use > ffd.machines[machine_count].cpu_capacity / 2 or \
                    (time_cpu_use > np.full(LINE_SIZE, ffd.machines[machine_count].cpu_capacity / 2)).any() \
                    or avg_mem_use > ffd.machines[machine_count].mem_capacity or \
                    (time_mem_use > np.full(LINE_SIZE, ffd.machines[machine_count].mem_capacity)).any():
                out_of_capacity = True
            if inst_count == 0:
                continue
            avg_cpu_use /= inst_count
            avg_mem_use /= inst_count
            result = "%s\t\t%4.3f\t\t%4.3f\t\t%4.3f\t\t%4.3f\n" % (out_of_capacity, max_cpu_use, avg_cpu_use, max_mem_use, avg_mem_use)
            results.append(result)

            score = 0
            for i in range(LINE_SIZE):
                score += (1 + 10 * (math.exp(max(0, (ffd.machines[machine_count].cpu_use[i] / ffd.machines[machine_count].cpu_capacity) - 0.5)) - 1))
            final_score += score / LINE_SIZE
            machine_count += 1
    print final_score
    ffd.fit_before(ffd.machines[0:machine_count])
    return results
