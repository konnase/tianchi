using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using static System.Math;
using static System.Console;

namespace Tianchi {
  #region Job 和 JobTask，两者是只读的

  public class Job {
    // 入口任务
    // 仅有1个入口任务的Job最多，
    // 大部分Job的入口任务少于15，
    // 所有Job的入口任务均不超过39个
    public readonly List<JobTask> BeginTasks = new List<JobTask>(capacity: 15);

    // 没有后继任务的那些任务
    public readonly List<JobTask> EndTasks = new List<JobTask>(capacity: 20);
    public readonly int Id;

    // <jobId, task>
    public readonly Dictionary<int, JobTask> TaskKv = new Dictionary<int, JobTask>(capacity: 25);

    public Job(int id) {
      Id = id;
    }

    public int TotalDuration { get; private set; }
    public int TaskCount { get; private set; }

    /// <summary>
    ///   注意：前驱一次即可全部读取（最多有17个前驱），但后继则不能
    ///   读取了Job所有实例后才能调用此方法
    /// </summary>
    public void UpdateTaskInfo() {
      TaskCount = TaskKv.Count;

      var cnt = int.MinValue;
      while (cnt != 0) {
        cnt = 0;
        foreach (var task in TaskKv.Values) {
          if (task.BeginEarliest == int.MinValue) {
            if (SetBeginEarliest(task)) {
              cnt++;
            }
          }
        }
      }

      var last = 0;
      foreach (var task in TaskKv.Values) {
        if (task.Post.Count == 0) {
          task.IsEndTask = true;
          EndTasks.Add(task);
        }

        var end = task.BeginEarliest + task.Duration;
        if (last < end) {
          last = end;
        }
      }

      TotalDuration = last;

      foreach (var task in EndTasks) {
        task.BeginLatest = TotalDuration - task.Duration;
      }

      cnt = int.MinValue;
      while (cnt != 0) {
        cnt = 0;
        foreach (var task in TaskKv.Values) {
          if (task.BeginLatest == int.MinValue) {
            if (SetBeginLatest(task)) {
              cnt++;
            }
          }
        }
      }
    }

    private static bool SetBeginEarliest(JobTask task) {
      var curBegin = int.MinValue;
      // 由调用者保证Prev不是null
      foreach (var prev in task.Prev) {
        if (prev.BeginEarliest == int.MinValue) {
          return false;
        }

        var prevEnd = prev.BeginEarliest + prev.Duration;
        if (curBegin < prevEnd) {
          curBegin = prevEnd;
        }
      }

      task.BeginEarliest = curBegin;
      return true;
    }

    private static bool SetBeginLatest(JobTask task) {
      var curEnd = int.MaxValue;
      foreach (var post in task.Post) {
        if (post.BeginLatest == int.MinValue) {
          return false;
        }

        if (curEnd > post.BeginLatest) {
          curEnd = post.BeginLatest;
        }
      }

      task.BeginLatest = curEnd - task.Duration;
      return true;
    }
  }

  public class JobTask {
    public readonly string FullId; // "jobId-taskId"
    public readonly int Id; // task id不含job id前缀
    public readonly Job Job;
    public readonly List<JobTask> Post = new List<JobTask>();

    /// <summary>
    ///   相对的最早开始时间
    /// </summary>
    public int BeginEarliest = int.MinValue;

    /// <summary>
    ///   相对的最晚开始时间
    /// </summary>
    public int BeginLatest = int.MinValue;

    public double Cpu;
    public int Duration; // 以分钟计的执行时间
    public int InstCount;
    public bool IsBeginTask;
    public bool IsEndTask;
    public double Mem;
    public JobTask[] Prev;

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

      var prevFullId = parts[5];
      //parts的第6个元素肯定存在，但可能为空串
      //空串表明没有前驱
      if (string.IsNullOrEmpty(prevFullId)) {
        task.BeginEarliest = 0;
        task.Prev = null;
        task.IsBeginTask = true;
        job.BeginTasks.Add(task);
        return;
      }

      var prevCnt = parts.Length - 5;
      task.Prev = new JobTask[prevCnt];
      //否则parts至少有一个非空的元素
      for (var i = 5; i < parts.Length; i++) {
        SetPrevTask(i - 5, parts[i], task);
      }
    }

    private static void SetPrevTask(int idx, string preFullId, JobTask task) {
      var prevTaskId = preFullId.IdPair().Item2;
      var job = task.Job;

      if (!job.TaskKv.TryGetValue(prevTaskId, out var prevTask)) {
        //先占坑，具体的资源值等读到对应行再填
        prevTask = new JobTask(job, preFullId, prevTaskId);
        job.TaskKv[prevTaskId] = prevTask;
      }

      task.Prev[idx] = prevTask;
      prevTask.Post.Add(task);
    }

    public override string ToString() {
      return $"{FullId},{Cpu},{Mem},{InstCount},{Duration}";
    }
  }

  #endregion

  #region JobBatch

  /// <summary>
  ///   某 JobTask 同时部署在同一机器的一组实例
  ///   部署时才能确定，不可变对象
  /// </summary>
  public class JobBatch {
    public readonly int BeginTime;
    public readonly Machine Machine;

    // 同时部署在同一机器的一组实例的个数，不大于 Task 的总 InstCount
    public readonly int Size;
    public readonly JobTask Task;

    // 创建一个新Batch即部署一组Task的实例到某台机器
    public JobBatch(JobTask task, Machine machine, int size, int beginTime) {
      Task = task;
      Machine = machine;
      Size = size;
      BeginTime = beginTime;
    }

    public Job Job => Task.Job;
    public string FullId => Task.FullId;
    public double Cpu => Task.Cpu;
    public double Mem => Task.Mem;
    public int Duration => Task.Duration;
    public int EndTime => BeginTime + Task.Duration;

    public override string ToString() {
      return $"{Task.FullId},machine_{Machine.Id},{BeginTime},{Size}";
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
        size = set.Sum(batch => batch.Size);
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
    ///   如果 task 全部部署了，返回最晚的完成时间，
    ///   否则返回 int.MinValue
    /// </summary>
    public static int EndTime(this JobTask task, Solution solution) {
      if (!solution.BatchKv.TryGetValue(task, out var set)) {
        return int.MinValue; //尚未部署
      }

      var totalCnt = 0;
      var last = int.MinValue;
      foreach (var batch in set) {
        totalCnt += batch.Size;
        if (last < batch.EndTime) {
          last = batch.EndTime;
        }
      }

      return totalCnt == task.InstCount ? last : int.MinValue; //没有全部部署
    }

    /// <summary>
    ///   返回前驱的结束时刻
    ///   如果没有前驱，返回0；如果前驱尚未全部部署，返回int.MinValue
    /// </summary>
    public static int EndTimeOfPrev(this JobTask task, Solution solution) {
      if (task.Prev == null) {
        return 0; //没有前驱
      }

      var last = int.MinValue;
      foreach (var prev in task.Prev) {
        var end = prev.EndTime(solution);
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

    // JobKv在部署过程中是只读的
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
    ///   各 JobTask 部署实例，即 Job 的部署信息
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

    public static bool CheckTaskSequence(Solution solution) {
      var batchKv = solution.BatchKv;
      foreach (var kv in batchKv) {
        var task = kv.Key;
        var end = task.EndTime(solution);

        foreach (var post in task.Post) {
          var postBatchSet = batchKv[post];
          foreach (var batch in postBatchSet) {
            //考虑到资源计算规则，BeginTime不能与end相同
            if (batch.BeginTime < end) {
              WriteLine($"[CheckTaskSequence]: {post.FullId}@m_{batch.Machine.Id} " +
                        $"begins before {task.FullId} finish!");
              return false;
            }
          }
        }
      }

      return true;
    }

    // 写完之后不关闭文件！  
    public static void SaveJobSubmit(Solution solution, StreamWriter writer) {
      foreach (var batchSet in solution.BatchKv.Values) {
        batchSet.ForEach(WriteLine);
      }
    }

    public static void ReadJobSubmit(string csvSubmit, Solution clone) {
      Util.ReadCsv(csvSubmit, parts => {
        if (parts.Length == 3) { //App有3部分，Job有4部分
          return true;
        }

        var task = clone.GetTask(parts[0]);
        var m = clone.MachineKv[parts[1].Id()];
        var beginTime = int.Parse(parts[2]);
        var size = int.Parse(parts[3]);
        if (m.TryPut(task, beginTime, size, out var batch, out _)) {
          if (batch.Size != size) {
            WriteLine($"[ReadJobSubmit]: {batch}, Actual batch size={batch.Size}!");
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

        WriteLine($"[ReadJobSubmit]: {batch}, Can not put task!");
        return false;
      });

      if (!clone.AllJobDeployed) {
        WriteLine("[ReadJobSubmit]: Not all tasks are deployed!");
      }
    }

    public static void SaveAndJudge(Solution final) {
      var csvSubmit = $"submit_{final.DataSet.Id}.csv";
      var writer = File.CreateText(csvSubmit);

      var clone = final.DataSet.InitSolution.Clone();
      TrySaveAppSubmit(final, clone, writer);
      SaveJobSubmit(final, writer);
      writer.Close();

      WriteLine($"== DataSet {final.DataSet.Id} Judge==");
      //从文件读入部署会改变clone的状态
      clone.SetInitAppDeploy();
      clone.ClearJobDeploy();

      ReadAppSubmit(csvSubmit, clone);
      ReadJobSubmit(csvSubmit, clone);
      Write($"[SaveAndJudge]: {clone.DataSet.Id} ");
      CheckAppInterference(clone);
      CheckResource(clone);
      CheckTaskSequence(clone);
      WriteLine(clone.ScoreMsg);
    }
  }

  public partial class Machine {
    /// <summary>
    ///   仅用于检查是否有重复部署
    ///   TODO: 这里假设一台机器只能部署Task的一个batch（实例个数，beginTime），
    ///   不考虑beginTime不同的batch并存
    /// </summary>
    public readonly Dictionary<JobTask, JobBatch> BatchKv = new Dictionary<JobTask, JobBatch>();

    public bool HasJob => BatchKv.Count > 0;

    /// <summary>
    ///   返回 task 可部署的实例个数，
    ///   如果无法部署，out 参数的 neckTs 是资源最紧张的那个时刻（目前没有用到该值）。
    ///   task 的使用资源是固定的，只有neckTs之后 *才可能* 有足够资源
    /// </summary>
    public int AvailBatchSize(JobTask task, int beginTime, out int neckTs,
      double cpuUtilLimit = 1.0) {
      //
      var maxTsCpu = _usage.Cpu.IndexOfMax(beginTime, task.Duration);
      var maxCpu = _usage.Cpu[maxTsCpu];
      var maxSizeByCpu = (int) ((CapCpu * cpuUtilLimit - maxCpu) / task.Cpu);
      var lackCpu = maxSizeByCpu < 1;

      var maxTsMem = _usage.Mem.IndexOfMax(beginTime, task.Duration);
      var maxMem = _usage.Mem[maxTsMem];
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

    public bool TryPut(JobTask task, int beginTime,
      int maxSize, // 实例个数既受资源限制，也不能超过task总的实例个数
      out JobBatch batch, out int neckTs,
      double cpuUtilLimit = 1.0) {
      //
      batch = null;

      var size = AvailBatchSize(task, beginTime, out neckTs, cpuUtilLimit);
      if (size < 1) {
        return false;
      }

      size = Min(size, maxSize);
      batch = Put(task, beginTime, size);

      return true;
    }

    public JobBatch Put(JobTask task, int beginTime, int size) {
      if (BatchKv.TryGetValue(task, out var x)) {
        throw new Exception(
          $"[Put]: {task.FullId} has {x.Size}@{x.BeginTime} min " +
          $"deployed on m_{Id} already");
      }

      var batch = new JobBatch(task, this, size, beginTime);
      BatchKv[task] = batch;

      _usage.Add(batch);
      _score = double.MinValue;
      _avail.Invalid();
      _xUsage.Invalid();

      return batch;
    }

    public void Remove(JobBatch batch) {
      if (BatchKv.Remove(batch.Task)) {
        _usage.Subtract(batch);
        _score = double.MinValue;
        _avail.Invalid();
        _xUsage.Invalid();
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
