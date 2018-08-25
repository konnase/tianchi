using System;
using System.Collections.Generic;
using System.Linq;

namespace Tianchi {
  //DataSet存储数据集只读的部分
  //但也有一份默认的机器和实例列表
  //并发执行多个部署方案时（如参数调优），可克隆一份机器和实例列表
  public class DataSet {
    public readonly int AppCount;
    public readonly int AppInstCount;
    public readonly DataSetId Id;
    public readonly int MachineCount;

    private DataSet(DataSetId dsId, int appCnt, int appInstCnt, int mCnt, string appInstCsv) {
      Id = dsId;
      AppCount = appCnt;
      AppInstCount = appInstCnt;
      MachineCount = mCnt;

      AppInstCsv = appInstCsv;
    }

    //Job个数不等于行数，5个数据集的Job个数分别为 1085,478,546,1094和0
    public int JobCount { get; private set; }

    //instCsv里有实例Id和部署信息
    public string AppInstCsv { get; }

    //App是只读的，每个数据集只需保存一份
    public Dictionary<int, App> AppKv { get; private set; }
    public Dictionary<int, Job> JobKv { get; private set; }

    // 初始状态
    // 不要直接修改 InitSolution，要先 Clone 一份，修改克隆！
    public Solution InitSolution { get; private set; }

    public static DataSet Read(DataSetId dataSetId, string appCsv, string xCsv,
      string machineCsv, string instCsv, string jobCsv = "", bool isAlpha10 = false) {
      //复赛的5个数据集共用App资源和冲突约束
      var appCnt = Ext.GetLineCount(appCsv);
      var appKv = new Dictionary<int, App>(appCnt);
      ReadApp(appCsv, appKv);
      ReadX(xCsv, appKv);

      return Read(dataSetId, appKv, machineCsv, instCsv, jobCsv, isAlpha10);
    }

    public static DataSet Read(DataSetId dataSetId, Dictionary<int, App> appKv,
      string machineCsv, string instCsv, string jobCsv = "", bool isAlpha10 = false) {
      var appCnt = appKv.Count;
      var instCnt = Ext.GetLineCount(instCsv);
      var mCnt = Ext.GetLineCount(machineCsv);

      var dataSet = new DataSet(dataSetId, appCnt, instCnt, mCnt, instCsv);

      dataSet.AppKv = appKv;
      if (string.IsNullOrEmpty(jobCsv)) {
        dataSet.JobKv = null;
        dataSet.JobCount = 0;
      } else {
        dataSet.JobKv = new Dictionary<int, Job>(1100);
        ReadJob(jobCsv, dataSet.JobKv);
        dataSet.JobCount = dataSet.JobKv.Count;
      }

      dataSet.InitSolution = Solution.Read(dataSet, machineCsv, instCsv, isAlpha10);
      return dataSet;
    }

    private static void ReadApp(string csv, Dictionary<int, App> appKv) {
      Ext.ReadCsv(csv, parts => appKv.Add(parts[0].Id(), App.Parse(parts)));
    }

    private static void ReadX(string csv, Dictionary<int, App> appKv) {
      Ext.ReadCsv(csv, parts => {
        var app = appKv[parts[0].Id()];
        var otherAppId = parts[1].Id();
        var k = int.Parse(parts[2]);

        app.AddXRule(otherAppId, k);
      });
    }

    private static void ReadJob(string csv, Dictionary<int, Job> jobKv) {
      Ext.ReadCsv(csv, parts => JobTask.Parse(parts, jobKv));

      foreach (var job in jobKv.Values) {
        var maxDur = 0;
        foreach (var task in job.TaskKv.Values) {
          var dur = task.RelStartTime + task.Duration;
          if (maxDur < dur) maxDur = dur;
        }

        job.TotalDuration = maxDur;
      }
    }

    #region Print Utils

    public void PrintInitStats() {
      PrintCsvInitInfo();
      Console.WriteLine();
      PrintRequestUtil();
      //Console.WriteLine();
      //PrintAppUtilStat();
      //Console.WriteLine();
      //Machine.PrintList(Solution.Machines.Where(m => !m.IsIdle));
      //Console.WriteLine();
      //PrintUtilTs();
    }

    public void PrintCsvInitInfo() {
      Console.WriteLine("==Init==");
      Console.WriteLine($"App#: {AppCount}, " +
                        $"AppInst#: {AppInstCount}, " +
                        $"Machine#: {MachineCount}");

      //以下输出初始数据中违反约束的情况
      Solution.AppFinalCheck(InitSolution, true);

      //初始数据中，忽略了约束，被强制部署到机器上的实例
      var xInstList = InitSolution.AppInsts
        .Where(i => i.Machine != null && !i.Deployed)
        .ToList();

      if (xInstList.Count > 0)
        Console.WriteLine($"xInstList.Count: {xInstList.Count}"); //DataSetA: 143

      var xMachineList = InitSolution.Machines
        .Where(m => !m.IsIdle && m.AppInstSet.Any(i => !i.Deployed))
        .ToList();

      if (xMachineList.Count > 0) Console.WriteLine($"xMachineList.Count: {xMachineList.Count}");

      //一台机器上可能有多个实例发生冲突，导致计数出入。
      //DataSet Pre A 共117台机器存在约束冲突
      foreach (var m in xMachineList)
      foreach (var x in m.ConflictList)
        Console.WriteLine($"m_{m.Id},A:app_{x.Item1.Id},B:app_{x.Item2.Id}," +
                          $"BCnt:{x.Item3} > BLimit:{x.Item4}");
    }

    //各时刻所有实例请求Cpu, Mem占总资源量的比例
    public void PrintRequestUtil() {
      var total = new Resource();
      InitSolution.Machines.ForEach(m => total.Add(m.Capacity));
      var totalCpu = total.Cpu[0];
      var totalMem = total.Mem[0];

      var req = new Resource();
      InitSolution.AppInsts.ForEach(inst => req.Add(inst.R));

      Console.WriteLine($"Disk: {req.Disk},{req.Disk * 100.0 / total.Disk:0.00}%");
      Console.WriteLine($"P: {req.P},{req.P * 100.0 / total.P:0.00}%");
      Console.WriteLine($"M: {req.M},{req.M * 100.0 / total.M:0.00}%");
      Console.WriteLine($"PM: {req.Pm},{req.Pm * 100.0 / total.Pm:0.00}%");

      Console.WriteLine();
      Console.WriteLine("ts,cpu,mem,util_cpu,util_mem");

      for (var i = 0; i < Resource.Ts1470; i++)
        Console.WriteLine($"{i + 1},{req.Cpu[i]:0.0},{req.Mem[i]:0.0}," +
                          $"{req.Cpu[i] * 100 / totalCpu:0.000}%," +
                          $"{req.Mem[i] * 100 / totalMem:0.000}%");
    }

    // public void PrintAppUtilStat() {
    //   Console.WriteLine("aid,inst_cnt," +
    //                     "cpu_max,cpu_avg,cpu_min,cpu_stdev," +
    //                     "mem_max,mem_avg,mem_min,mem_stedv," +
    //                     "disk,p,m,pm");
    //   foreach (var a in AppKv.Values.OrderBy(a => a.Id)) {
    //     var r = a.R;
    //     Console.WriteLine($"app_{a.Id},{a.InstCount}," +
    //                       $"{r.Cpu.Max:0.0},{r.Cpu.Avg:0.0},{r.Cpu.Min:0.0},{r.Cpu.Stdev:0.0}," +
    //                       $"{r.Mem.Max:0.0},{r.Mem.Avg:0.0},{r.Mem.Min:0.0},{r.Mem.Stdev:0.0}," +
    //                       $"{r.Disk},{r.P},{r.M},{r.Pm}");
    //   }
    // }

    // // 这里cpu输出合计最大时刻ts=45的值，mem则是平均值，只使用初赛 DataSet B
    // public void PrintInstRequest() {
    //   Console.WriteLine(AppInstCount);
    //   foreach (var inst in InitSolution.AppInsts)
    //     Console.WriteLine($"{Math.Ceiling(inst.R.Cpu[45])} " +
    //                       $"{Math.Ceiling(inst.R.Mem.Avg)} " +
    //                       $"{inst.R.Disk} " +
    //                       $"inst_{inst.Id}");
    // }

    // // 将CPU和Mem分时数据转置一下
    // public void PrintUtilTs() {
    //   Console.WriteLine($"aid,ts,cpu,mem");
    //   foreach (var a in AppKv.Values.OrderBy(a => a.Id))
    //     for (var i = 0; i < a.R.Cpu.Length; i++)
    //       Console.WriteLine($"{a.Id},{i + 1},{a.R.Cpu[i]},{a.R.Mem[i]}");
    // }

    #endregion
  }
}
