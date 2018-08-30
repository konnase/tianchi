using System.Collections.Generic;

namespace Tianchi {
  // TODO: 这里限制实际的总执行时间与 job.TotalDuration 相同，即不存在等待
  // 由于成本分数不涉及Job的执行时间，只涉及资源，这个限制不是必要的
  // 而且一台机器上只能有同一Task的一个batch，不同起始时刻的batch不能并存
  public static class JobDeploy {
    // TODO: tuning this
    public static double CpuUtilLimit = 1.0;

    public static void FirstFit(Solution solution) {
      var jobKv = solution.DataSet.JobKv;

      //TODO: 对job和machine排序

      foreach (var job in jobKv.Values) {
        if (!TryDeployJobBegin(job, solution)) {
          continue; // 连合适的起始时间都无法确定，只好继续处理下一个 job
        }

        var restTaskCnt = job.TaskCount - job.BeginTasks.Count;
        var deployedCnt = 0;
        while (deployedCnt != restTaskCnt) {
          var cnt = DeployJobRest(job, solution);
          if (cnt == 0) { //其它的 Task 也无法部署了，只好继续处理下一个 job
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
        if (task.IsBeginTask) {
          continue;
        }

        if (task.IsDeployed(solution)) {
          continue;
        }

        var begin = task.EndTimeOfPrev(solution);
        if (begin == int.MinValue) { //前驱尚未全部部署
          continue;
        }

        if (TryDeploy(task, begin, solution)) {
          deployedCnt++;
        }
      }

      return deployedCnt;
    }

    // 搜索并部署Job的可行的开始时刻 
    private static bool TryDeployJobBegin(Job job, Solution solution) {
      var end = Resource.T1470 - job.TotalDuration;
      for (var begin = 0; begin < end; begin++) {
        // Job 起始 tasks 开始时刻可以在 0 ~ T1470-Job.TotalDuration
        var deployedCnt = 0;
        foreach (var task in job.BeginTasks) {
          if (TryDeploy(task, begin, solution)) {
            deployedCnt++;
          }
        }

        if (deployedCnt == job.BeginTasks.Count) {
          return true;
        }

        //如果当前时刻不合适，则尝试下一个时刻，需要清空之前的部署
        foreach (var task in job.BeginTasks) {
          task.Remove(solution);
        }
      }

      return false;
    }

    /// <summary>
    ///   将 task 的所有实例部署到多个机器上，
    ///   起始时间在 begin + [Earliest ~ Latest] 区间内
    /// </summary>
    private static bool TryDeploy(JobTask task, int begin, Solution solution) {
      var batchKv = solution.BatchKv;
      var machines = solution.Machines;
      var deployed = false;

      // latest 肯定不大于end；对初始任务，BeginEarliest == 0;
      var latest = begin + task.BeginLatest;
      for (var t = begin; t <= latest; t++) {
        var maxSize = task.UndeployedInstCount(solution);

        foreach (var m in machines) {
          // First Fit
          // 在m的begin时刻无法部署，可以换一台机器，也可以尝试neckTs的下一时刻，这里换机器
          if (!m.TryPut(task, begin, maxSize, out var batch, out _, CpuUtilLimit)) {
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

          // else // batch.Size == maxSize
          deployed = true; // 一个task的全部实例都部署了
          break; // foreach machines
        }

        if (deployed) {
          break; // foreach t 
        }

        // 否则尝试下一时刻，
        // 故同一task的实例既可能部署在不同机器，也可能是不同的启动时刻
        // 但限制了时间区间，不会导致拖长本阶段的截止时间
      }

      return deployed;
    }
  }
}
