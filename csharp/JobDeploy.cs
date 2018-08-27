using System.Collections.Generic;

namespace Tianchi {
  public static class JobDeploy {
    public static double CpuUtilLimit = 1.0;

    public static void FirstFit(Solution solution) {
      var jobKv = solution.DataSet.JobKv;

      //TODO: 对job和machine排序

      foreach (var job in jobKv.Values) {
        if (!TryDeployJobStart(job, solution)) {
          continue; //处理下一个job
        }

        var restTaskCnt = job.TaskCount - job.StartTasks.Count;
        var deployedCnt = 0;
        while (deployedCnt != restTaskCnt) {
          var cnt = DeployJobRest(job, solution);
          if (cnt == 0) {
            break;
          }

          deployedCnt += cnt;
        }
      }
    }

    /// <summary>
    ///   部署Job非起始的task，返回部署成功的task个数
    /// </summary>
    private static int DeployJobRest(Job job, Solution solution) {
      var deployedCnt = 0;
      foreach (var task in job.TaskKv.Values) {
        if (task.IsStartTask) {
          continue;
        }

        var start = task.EndTimeOfPre(solution);
        if (start == int.MinValue) {
          continue;
        }

        if (task.IsDeployed(solution)) {
          continue;
        }

        if (TryDeploy(task, start, solution)) {
          deployedCnt++;
        }
      }

      return deployedCnt;
    }

    // 搜索并部署Job的可行的开始时刻 
    private static bool TryDeployJobStart(Job job, Solution solution) {
      var end = Resource.T1470 - job.TotalDuration;
      for (var start = 0; start < end; start++) {
        // Job 起始 tasks 开始时刻可以在 0 ~ T1470-Job.TotalDuration
        var deployedCnt = 0;
        foreach (var task in job.StartTasks) {
          if (TryDeploy(task, start, solution)) {
            deployedCnt++;
          }
        }

        if (deployedCnt == job.StartTasks.Count) {
          return true;
        }

        //如果当前时刻不合适，则尝试下一个时刻，需要清空之前的部署
        foreach (var task in job.StartTasks) {
          task.Remove(solution);
        }
      }

      return false;
    }

    /// <summary>
    ///   将 task 的所有实例部署到多个机器上，
    ///   起始时间在 start + [Earliest ~ Latest] 区间内
    /// </summary>
    private static bool TryDeploy(JobTask task, int start, Solution solution) {
      var batchKv = solution.BatchKv;
      var machines = solution.Machines;
      var deployed = false;

      // latest 肯定不大于end；对初始任务，StartEarliest == 0;
      var latest = start + task.StartLatest;
      for (var t = start; t <= latest; t++) {
        var maxSize = task.UndeployedInstCount(solution);

        foreach (var m in machines) {
          // First Fit
          // 在m的start时刻无法部署，可以换一台机器，也可以尝试neckTs的下一时刻，这里换机器
          if (!m.TryPut(task, start, maxSize, out var batch, out _, CpuUtilLimit)) {
            continue;
          }

          if (!batchKv.TryGetValue(task, out var set)) {
            set = new HashSet<JobBatch>();
            batchKv[task] = set;
          }

          set.Add(batch);
          if (batch.Size != maxSize) {
            maxSize -= batch.Size;
            continue;
          }

          deployed = true; // 一个task的全部实例都部署了
          break; // foreach machines
        }

        if (deployed) {
          break; // foreach t 
        }

        // 否则尝试下一时刻，
        // 故同一task的实例既可能部署在不同机器，也可能是不同的启动时刻
        // 但限制了时间区间不会导致拖长本阶段的截止时间
      }

      return deployed;
    }
  }
}
