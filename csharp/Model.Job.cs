using System.Collections.Generic;
using System.Diagnostics;

namespace Tianchi {
  public class Job {
    public readonly int Id;

    // 入口任务，仅有1个入口任务的Job最多，
    // 大部分Job的入口任务少于15，所有Job入口任务不大于39个
    public readonly List<JobTask> StartTasks = new List<JobTask>(15);

    public readonly Dictionary<int, JobTask> TaskKv = new Dictionary<int, JobTask>(25);
    public double TotalDuration;

    public Job(int id) {
      Id = id;
    }
  }

  public class JobTask {
    // cache，避免gc，非线程安全！ task最多有17个前驱
    private static readonly List<JobTask>
      CacheList = new List<JobTask>(18);

    public readonly string FullId;
    public readonly int Id; // task id不包括job id前缀
    public readonly Job Job;
    public readonly List<JobTask> Post = new List<JobTask>();
    public double Cpu;
    public int Duration; // 以分钟计的执行时间
    public int InstCount; // task也有实例！
    public double Mem;
    public JobTask[] Prev; // 前驱一次即可读取，但后继则不能
    public int RelStartTime; // 相对于本Job的最早开始时间

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
    //1018-2,0.50,1.00,1,144,1018-1
    public static void Parse(string[] fields, Dictionary<int, Job> jobKv) {
      var idPairStr = fields[0];
      var idPair = idPairStr.IdPair();
      var jobId = idPair.Item1;
      var taskId = idPair.Item2;

      var cpu = double.Parse(fields[1]);
      var mem = double.Parse(fields[2]);
      var instCnt = int.Parse(fields[3]);
      var dur = int.Parse(fields[4]);

      if (!jobKv.ContainsKey(jobId)) jobKv[jobId] = new Job(jobId);

      var job = jobKv[jobId];
      JobTask task;
      if (!job.TaskKv.ContainsKey(taskId)) {
        task = new JobTask(job, idPairStr, taskId, cpu, mem, instCnt, dur);
        job.TaskKv[taskId] = task;
      } else { //否则，该task是某个task的前驱，已经占坑了，要设置资源值
        task = job.TaskKv[taskId];
        task.Cpu = cpu;
        task.Mem = mem;

        task.Duration = dur;
      }

      CacheList.Clear();

      var preIdPairStr = fields[5]; //fields的第6个元素肯定存在，但可能为空串
      //空串表明没有前驱
      if (string.IsNullOrEmpty(preIdPairStr)) {
        task.RelStartTime = 0; //task.Prev = null;
        job.StartTasks.Add(task);
        return;
      }

      //如果fields至少有一个元素
      for (var i = 5; i < fields.Length; i++) SetPreTaskList(fields[i], job, task);

      CacheList.Sort((t1, t2) => t1.Id.CompareTo(t2.Id));
      task.Prev = CacheList.ToArray();

      var earliest = int.MinValue;
      foreach (var pre in task.Prev) {
        var end = pre.RelStartTime + pre.Duration;
        if (earliest < end) earliest = end;
      }

      task.RelStartTime = earliest;
    }

    private static void SetPreTaskList(string preIdPairStr, Job job, JobTask task) {
      var preIdPair = preIdPairStr.IdPair();
      Debug.Assert(preIdPair.Item1 == job.Id);
      var preTaskId = preIdPair.Item2;

      JobTask preTask;
      if (!job.TaskKv.ContainsKey(preTaskId)) {
        preTask = new JobTask(job, preIdPairStr, preTaskId); //先占坑，具体的资源值等读到对应行再填
        job.TaskKv[preTaskId] = preTask;
      } else {
        preTask = job.TaskKv[preTaskId];
      }

      CacheList.Add(preTask);
      preTask.Post.Add(task);
    }
  }

  public class JobInst {
    public readonly Job Job;
    public readonly JobTask JobTask;
    public int InstCount; // 一起部署到某个机器上的实例个数，要不大于Task的总InstCount
    public Machine Machine;
    public int StartTime; // 部署后实际的开始时间

    public JobInst(JobTask task) {
      JobTask = task;
      Job = task.Job;
    }

    public double Cpu => JobTask.Cpu;
    public double Mem => JobTask.Mem;
    public double Duration => JobTask.Duration;
  }

  public partial class Machine {
    //机器与Job相关的代码都放到这里吧

    public Dictionary<JobTask, JobInst> BatchInsts = new Dictionary<JobTask, JobInst>();

    public bool TryPutBatchInst() {
      return false;
    }
  }
}
