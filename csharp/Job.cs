using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using static System.Math;
using static System.Console;

namespace Tianchi {
  #region Job 和 JobTask，两者是只读的

  public class Job {
    // 没有后继任务的那些任务的列表
    public readonly List<JobTask> EndTasks = new List<JobTask>(20);
    public readonly int Id;

    // 入口任务，仅有1个入口任务的Job最多，
    // 大部分Job的入口任务少于15，所有Job入口任务不大于39个
    public readonly List<JobTask> StartTasks = new List<JobTask>(15);

    // <taskId, task>
    public readonly Dictionary<int, JobTask> TaskKv = new Dictionary<int, JobTask>(25);

    public Job(int id) {
      Id = id;
    }

    public int TotalDuration { get; private set; }
    public int TaskCount { get; private set; }

    /// <summary>
    ///   注意：因为前驱一次即可全部读取（最多有17个前驱），但后继则不能
    ///   所以读取了Job所有实例后才能调用此方法
    /// </summary>
    public void UpdateTaskInfo() {
      TaskCount = TaskKv.Count;

      var earliestCnt = 0;
      while (earliestCnt != TaskCount) {
        earliestCnt = 0;
        foreach (var task in TaskKv.Values) {
          if (task.StartEarliest == int.MinValue) {
            if (SetStartEarliest(task)) {
              earliestCnt++;
            }
          } else {
            earliestCnt++;
          }
        }
      }

      var maxDur = 0;
      foreach (var task in TaskKv.Values) {
        if (task.Post.Count == 0) {
          task.IsEndTask = true;
          EndTasks.Add(task);
        }

        var dur = task.StartEarliest + task.Duration;
        if (maxDur < dur) {
          maxDur = dur;
        }
      }

      TotalDuration = maxDur;

      foreach (var task in EndTasks) {
        task.StartLatest = TotalDuration - task.Duration;
      }

      var latestCnt = 0;
      while (latestCnt != TaskCount) {
        latestCnt = 0;
        foreach (var task in TaskKv.Values) {
          if (task.StartLatest == int.MinValue) {
            if (SetStartLatest(task)) {
              latestCnt++;
            }
          } else {
            latestCnt++;
          }
        }
      }
    }

    private static bool SetStartEarliest(JobTask task) {
      var curStart = int.MinValue;
      foreach (var pre in task.Prev) {
        if (pre.StartEarliest == int.MinValue) {
          return false;
        }

        var end = pre.StartEarliest + pre.Duration;
        if (curStart < end) {
          curStart = end;
        }
      }

      task.StartEarliest = curStart;
      return true;
    }

    private static bool SetStartLatest(JobTask task) {
      var curEnd = int.MaxValue;
      foreach (var post in task.Post) {
        if (post.StartLatest == int.MinValue) {
          return false;
        }

        if (curEnd > post.StartLatest) {
          curEnd = post.StartLatest;
        }
      }

      task.StartLatest = curEnd - task.Duration;
      return true;
    }
  }

  public class JobTask {
    public readonly string FullId; // "jobId-taskId"
    public readonly int Id; // task id不含job id前缀
    public readonly Job Job;
    public readonly List<JobTask> Post = new List<JobTask>();
    public double Cpu;
    public int Duration; // 以分钟计的执行时间
    public int InstCount;
    public bool IsEndTask;
    public bool IsStartTask;
    public double Mem;
    public JobTask[] Prev;

    /// <summary>
    ///   相对的最早开始时间
    /// </summary>
    public int StartEarliest = int.MinValue;

    /// <summary>
    ///   相对的最晚开始时间
    /// </summary>
    public int StartLatest = int.MinValue;

    public JobTask(Job job, string fullId, int id,
      double cpu = 0.0, double mem = 0.0, int instCnt = 0, int dur = 0) {
      Job = job;
      FullId = fullId;
      Id = id;
      Cpu = cpu;
      Mem = mem;
      InstCount = instCnt;
      Duration = dur;
    }

    //1018-1,0.50,0.50,3,144,
    //1018-2,0.50,1.00,1,144,1018-1,1018-x
    public static void Parse(string[] parts, Dictionary<int, Job> jobKv) {
      var fullId = parts[0];
      var idPair = fullId.IdPair();
      var jobId = idPair.Item1;
      var taskId = idPair.Item2;

      var cpu = double.Parse(parts[1]);
      var mem = double.Parse(parts[2]);
      var instCnt = int.Parse(parts[3]);
      var dur = int.Parse(parts[4]);

      if (!jobKv.TryGetValue(jobId, out var job)) {
        job = new Job(jobId);
        jobKv[jobId] = job;
      }

      if (!job.TaskKv.TryGetValue(taskId, out var task)) {
        task = new JobTask(job, fullId, taskId, cpu, mem, instCnt, dur);
        job.TaskKv[taskId] = task;
      } else { //否则，该task是某个的前驱，已经填在坑里了，更新资源值
        task.Cpu = cpu;
        task.Mem = mem;
        task.InstCount = instCnt;
        task.Duration = dur;
      }

      var preIdPairStr = parts[5];
      //parts的第6个元素肯定存在，但可能为空串
      //空串表明没有前驱
      if (string.IsNullOrEmpty(preIdPairStr)) {
        task.StartEarliest = 0;
        task.Prev = null;
        task.IsStartTask = true;
        job.StartTasks.Add(task);
        return;
      }

      var preCnt = parts.Length - 5;
      task.Prev = new JobTask[preCnt];
      //否则parts至少有一个非空的元素
      for (var i = 5; i < parts.Length; i++) {
        SetPreTask(i - 5, parts[i], job, task);
      }
    }

    private static void SetPreTask(int idx, string preFullId, Job job, JobTask task) {
      var preTaskId = preFullId.IdPair().Item2;

      if (!job.TaskKv.TryGetValue(preTaskId, out var preTask)) {
        preTask = new JobTask(job, preFullId, preTaskId); //先占坑，具体的资源值等读到对应行再填
        job.TaskKv[preTaskId] = preTask;
      }

      task.Prev[idx] = preTask;
      preTask.Post.Add(task);
    }
  }

  #endregion

  #region JobBatch

  /// <summary>
  ///   某 JobTask 同时部署在同一机器的一组实例
  ///   部署时才能确定，不可变对象
  /// </summary>
  public class JobBatch {
    public readonly Job Job;
    public readonly JobTask JobTask;

    public readonly Machine Machine;

    // 同时部署在同一机器的一组实例的个数，不大于 Task 的总 InstCount
    public readonly int Size;

    public readonly int StartTime;

    public JobBatch(JobTask task, Machine machine, int size, int startTime) {
      JobTask = task;
      Job = task.Job;
      Machine = machine;
      Size = size;
      StartTime = startTime;
    }

    public string FullId => JobTask.FullId;
    public double Cpu => JobTask.Cpu;
    public double Mem => JobTask.Mem;
    public int Duration => JobTask.Duration;
    public int EndTime => StartTime + JobTask.Duration;

    public override string ToString() {
      return $"{JobTask.FullId},machine_{Machine.Id},{StartTime},{Size}";
    }
  }

  #endregion

  /// <summary>
  ///   Job部署方案（Solution）相关的扩展方法
  /// </summary>
  public static class JobExtension {
    public static int UndeployedInstCount(this JobTask task, Solution solution) {
      var size = 0;
      if (solution.BatchKv.TryGetValue(task, out var set)) {
        foreach (var batch in set) {
          size += batch.Size;
        }
      }

      //部署算法正确的话，不会出现负值
      return task.InstCount - size;
    }

    public static bool IsDeployed(this JobTask task, Solution solution) {
      return task.UndeployedInstCount(solution) == 0;
    }

    public static bool IsDeployed(this Job job, Solution solution) {
      return job.TaskKv.All(kv => kv.Value.IsDeployed(solution));
    }

    /// <summary>
    ///   如果 task 全部部署了，返回最后一个batch的完成时间，
    ///   否则返回 int.MinValue
    /// </summary>
    public static int EndTime(this JobTask task, Solution solution) {
      if (!solution.BatchKv.TryGetValue(task, out var list)) {
        return int.MinValue; //尚未部署
      }

      var sum = 0;
      var last = int.MinValue;
      foreach (var batch in list) {
        sum += batch.Size;
        if (last < batch.EndTime) {
          last = batch.EndTime;
        }
      }

      return sum == task.InstCount ? last : int.MinValue; //没有全部部署
    }

    /// <summary>
    ///   返回前驱的结束时刻
    ///   如果没有前驱，返回0；如果前驱尚未全部部署，返回int.MinValue
    /// </summary>
    public static int EndTimeOfPre(this JobTask task, Solution solution) {
      if (task.Prev == null) {
        return 0; //没有前驱
      }

      var last = int.MinValue;
      foreach (var pre in task.Prev) {
        var end = pre.EndTime(solution);
        if (end == int.MinValue) {
          return int.MinValue; //前驱尚未全部部署
        }

        if (last < end) {
          last = end;
        }
      }

      return last;
    }

    public static void Remove(this JobTask task, Solution solution) {
      var batchKv = solution.BatchKv;
      if (!batchKv.TryGetValue(task, out var set)) {
        return;
      }

      foreach (var batch in set) {
        batch.Machine.Remove(batch);
      }

      batchKv.Remove(task);
    }
  }

  // 使用partial class，把job相关的代码都放到一起
  public partial class DataSet {
    //Job个数不等于行数，5个数据集的Job个数分别为 1085,478,546,1094和0

    public Dictionary<int, Job> JobKv { get; private set; }

    private static void ReadJob(string csv, Dictionary<int, Job> jobKv) {
      Util.ReadCsv(csv, parts => JobTask.Parse(parts, jobKv));

      foreach (var job in jobKv.Values) {
        job.UpdateTaskInfo();
      }
    }
  }

  public partial class Solution {
    private Dictionary<JobTask, HashSet<JobBatch>> _batchKv;
    public int MachineCountHasJob => Machines.Sum(m => m.HasJob ? 1 : 0);

    public Dictionary<int, Job> JobKv => DataSet.JobKv;
    public int JobTaskCount => JobKv.Sum(kv => kv.Value.TaskCount);

    /// <summary>
    ///   各 JobTask 的部署实例列表
    /// </summary>
    public Dictionary<JobTask, HashSet<JobBatch>> BatchKv =>
      _batchKv ?? (_batchKv = new Dictionary<JobTask, HashSet<JobBatch>>(JobTaskCount));

    public bool AllJobDeployed => JobKv.All(kv => kv.Value.IsDeployed(this));

    public JobTask GetTask(string fullId) {
      var idPair = fullId.IdPair();
      var job = JobKv[idPair.Item1];
      return job.TaskKv[idPair.Item2];
    }

    public void ClearJobDeploy() {
      Machines.ForEach(m => m.ClearJobs());
    }

    // 写完之后不关闭文件！  
    public static void SaveJobSubmit(Solution solution, StreamWriter writer) {
      foreach (var batchList in solution.BatchKv.Values)
      foreach (var batch in batchList) {
        writer.WriteLine(batch);
      }
    }

    public static void ReadJobSubmit(string csvSubmit, Solution clone) {
      var foundJobSubmit = false;
      Util.ReadCsv(csvSubmit, line => {
        if (line[0] == '#') {
          foundJobSubmit = true;
          return true;
        }

        if (!foundJobSubmit) {
          return true;
        }

        var parts = line.Split(',');
        var task = clone.GetTask(parts[0]);
        var m = clone.MachineKv[parts[1].Id()];
        var startTime = int.Parse(parts[2]);
        var size = int.Parse(parts[3]);
        if (m.TryPut(task, startTime, size, out var batch, out _)) {
          if (batch.Size != size) {
            WriteLine($"[JudgeJobSubmit]: {line}, Actual batch size={batch.Size}");
            return false;
          }

          var batchKv = clone.BatchKv;
          if (!batchKv.TryGetValue(task, out var set)) {
            set = new HashSet<JobBatch>();
            batchKv[task] = set;
          }

          set.Add(batch);

          return true;
        }

        WriteLine($"[JudgeJobSubmit]: {line}, Cannot put task");
        return false;
      });
    }

    public static void SaveAndJudge(Solution final) {
      var csvSubmit = $"submit_{final.DataSet.Id}.csv";
      var writer = File.CreateText(csvSubmit);

      var clone = final.DataSet.InitSolution.Clone();
      SaveAppSubmit(final, clone, writer);
      writer.WriteLine("#");
      SaveJobSubmit(final, writer);
      writer.Close();

      WriteLine($"== DataSet {final.DataSet.Id} Judge==");
      //从文件读入的部署会改变init的状态

      clone.SetInitAppDeploy();
      clone.ClearJobDeploy();

      ReadAppSubmit(csvSubmit, clone);
      ReadJobSubmit(csvSubmit, clone);
      Write($"[SaveAndJudge]: {clone.DataSet.Id} ");

      WriteLine(clone.ScoreMsg);
    }
  }

  public partial class Machine {
    public readonly Dictionary<JobTask, JobBatch> BatchKv = new Dictionary<JobTask, JobBatch>();
    public bool HasJob => BatchKv.Count > 0;

    /// <summary>
    ///   返回 task 可部署的实例个数，
    ///   如果无法部署，out 参数的 neckTs 是资源最紧张的那个时刻（目前没有用到该值）。
    ///   task 的使用资源是固定的，只有neckTs之后 *才可能* 有足够资源
    /// </summary>
    public int CalcAvailBatchSize(JobTask task, int startTime, out int neckTs,
      double cpuUtilLimit = 1.0) {
      //
      var maxTsCpu = _usage.Cpu.IndexOfMax(startTime, task.Duration);
      var maxCpu = _usage.Cpu[maxTsCpu];
      var maxSizeByCpu = (int) ((CapCpu * cpuUtilLimit - maxCpu) / task.Cpu);
      var lackCpu = maxSizeByCpu < 1;

      var maxTsMem = _usage.Mem.IndexOfMax(startTime, task.Duration);
      var maxMem = _usage.Mem[maxTsCpu];
      var maxSizeByMem = (int) ((CapMem - maxMem) / task.Mem);
      var lackMem = maxSizeByMem < 1;

      neckTs = int.MinValue;
      if (!lackCpu && !lackMem) {
        return Min(maxSizeByCpu, maxSizeByMem);
      }

      if (lackCpu && lackMem) {
        neckTs = Max(maxTsCpu, maxTsMem);
      } else if (lackCpu) {
        neckTs = maxTsCpu;
      } else {
        neckTs = maxTsMem;
      }

      return 0;
    }

    public bool TryPut(JobTask task, int startTime,
      int maxSize, // 实例个数既受资源限制，也不能超过task总的实例个数
      out JobBatch batch, out int neckTs,
      double cpuUtilLimit = 1.0) {
      //
      Debug.Assert(maxSize > 0);
      batch = null;

      var size = CalcAvailBatchSize(task, startTime, out neckTs, cpuUtilLimit);
      if (size < 1) {
        return false;
      }

      size = Min(size, maxSize);
      batch = Put(task, startTime, size);

      return true;
    }

    /// <summary>
    ///   不做检查（时间，资源），直接放置
    /// </summary>
    public JobBatch Put(JobTask task, int startTime, int size) {
      if (BatchKv.ContainsKey(task)) {
        throw new Exception($"[Put]: {task.FullId} already has batch deployed on m_{Id}");
      }

      var batch = new JobBatch(task, this, size, startTime);
      BatchKv[task] = batch;

      _usage.Add(batch);
      _score = double.MinValue;
      return batch;
    }

    public void Remove(JobBatch batch) {
      if (BatchKv.Remove(batch.JobTask)) {
        _usage.Subtract(batch);
        _score = double.MinValue;
      } else {
        throw new Exception($"[Remove]: cannot find {batch.FullId} on m_{Id}");
      }
    }

    public void ClearJobs() {
      var batches = BatchKv.Values.ToList();
      foreach (var batch in batches) {
        Remove(batch);
      }
    }
  }
}
