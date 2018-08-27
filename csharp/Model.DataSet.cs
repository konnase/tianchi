using System.Collections.Generic;
using System.Linq;
using static System.Console;
using static System.Math;

namespace Tianchi {
  //DataSet存储数据集只读的部分
  //但也有一份默认的机器和实例列表
  //并发执行多个部署方案时（如参数调优），可克隆一份机器和实例列表
  public partial class DataSet {
    public readonly int AppCount;
    public readonly int AppInstCount;
    public readonly DataSetId Id;
    public readonly int MachineCount;

    private DataSet(DataSetId dsId, int appCnt, int appInstCnt, int mCnt) {
      Id = dsId;
      AppCount = appCnt;
      AppInstCount = appInstCnt;
      MachineCount = mCnt;
    }

    public Dictionary<int, App> AppKv { get; private set; }

    // 初始状态
    // 不要直接修改 InitSolution，要先 Clone 一份，修改克隆！
    public Solution InitSolution { get; private set; }

    public static DataSet Read(DataSetId dataSetId, string appCsv, string xCsv,
      string machineCsv, string instCsv, string jobCsv = "", bool isAlpha10 = false) {
      //复赛的5个数据集共用App资源和冲突约束
      var appCnt = Util.GetLineCount(appCsv);
      var appKv = new Dictionary<int, App>(appCnt);
      ReadApp(appCsv, appKv);
      ReadX(xCsv, appKv);

      return Read(dataSetId, appKv, machineCsv, instCsv, jobCsv, isAlpha10);
    }

    public static DataSet Read(DataSetId dataSetId, Dictionary<int, App> appKv,
      string machineCsv, string instCsv, string jobCsv = "", bool isAlpha10 = false) {
      var appCnt = appKv.Count;
      var instCnt = Util.GetLineCount(instCsv);
      var mCnt = Util.GetLineCount(machineCsv);

      var dataSet = new DataSet(dataSetId, appCnt, instCnt, mCnt) {AppKv = appKv};

      if (string.IsNullOrEmpty(jobCsv)) {
        dataSet.JobKv = null;
      } else {
        dataSet.JobKv = new Dictionary<int, Job>(1100);
        ReadJob(jobCsv, dataSet.JobKv);
      }

      dataSet.InitSolution = Solution.Read(dataSet, machineCsv, instCsv, isAlpha10);
      return dataSet;
    }

    private static void ReadApp(string csv, Dictionary<int, App> appKv) {
      Util.ReadCsv(csv, parts => appKv.Add(parts[0].Id(), App.Parse(parts)));
    }

    private static void ReadX(string csv, Dictionary<int, App> appKv) {
      Util.ReadCsv(csv, parts => {
        var app = appKv[parts[0].Id()];
        var otherAppId = parts[1].Id();
        var k = int.Parse(parts[2]);

        app.AddXRule(otherAppId, k);
      });
    }

    #region Print Utils

    public void PrintInitStats() {
      PrintCsvInitInfo();
      WriteLine();
      PrintRequestUtil();
      WriteLine();
      PrintAppUtilStat();
      WriteLine();
      Machine.PrintList(InitSolution.Machines.Where(m => m.HasApp));
      WriteLine();
      PrintAvgUtilByTs();
    }

    public void PrintCsvInitInfo() {
      WriteLine("==Init==");
      WriteLine($"App#: {AppCount}, " +
                $"AppInst#: {AppInstCount}, " +
                $"Machine#: {MachineCount}");

      //以下输出初始数据中违反约束的情况
      Solution.FinalCheckApp(InitSolution, true);

      //初始数据中，忽略了约束，被强制部署到机器上的实例
      var xInsts = InitSolution.AppInsts
        .Where(i => i.Machine != null && !i.IsDeployed)
        .ToList();

      if (xInsts.Count > 0) {
        WriteLine($"xInstList.Count: {xInsts.Count}"); //DataSetA: 143
      }

      var xMachineList = InitSolution.Machines
        .Where(m => m.HasApp && m.AppInstSet.Any(i => !i.IsDeployed))
        .ToList();

      if (xMachineList.Count > 0) {
        WriteLine($"xMachineList.Count: {xMachineList.Count}");
      }

      //一台机器上可能有多个实例发生冲突，导致计数出入。
      //DataSet Pre A 共117台机器存在约束冲突
      foreach (var m in xMachineList)
      foreach (var x in m.ConflictList) {
        WriteLine($"m_{m.Id},A:app_{x.Item1.Id},B:app_{x.Item2.Id}," +
                  $"BCnt:{x.Item3} > BLimit:{x.Item4}");
      }
    }

    //各时刻所有实例请求Cpu, Mem占总资源量的比例
    public void PrintRequestUtil() {
      var total = new Resource();
      InitSolution.Machines.ForEach(m => total.Add(m.Capacity));
      var totalCpu = total.Cpu[0];
      var totalMem = total.Mem[0];

      var req = new Resource();
      InitSolution.AppInsts.ForEach(inst => req.Add(inst.R));

      WriteLine($"Disk: {req.Disk},{req.Disk * 100.0 / total.Disk:0.00}%");
      WriteLine($"P: {req.P},{req.P * 100.0 / total.P:0.00}%");
      WriteLine($"M: {req.M},{req.M * 100.0 / total.M:0.00}%");
      WriteLine($"PM: {req.Pm},{req.Pm * 100.0 / total.Pm:0.00}%");

      WriteLine();
      WriteLine("ts,cpu,mem,util_cpu,util_mem");

      for (var i = 0; i < Resource.T1470; i++) {
        WriteLine($"{i + 1},{req.Cpu[i]:0.0},{req.Mem[i]:0.0}," +
                  $"{req.Cpu[i] * 100 / totalCpu:0.000}%," +
                  $"{req.Mem[i] * 100 / totalMem:0.000}%");
      }
    }

    public void PrintAppUtilStat() {
      WriteLine("aid,inst_cnt," +
                "cpu_max,cpu_avg,cpu_min,cpu_stdev," +
                "mem_max,mem_avg,mem_min,mem_stedv," +
                "disk,p,m,pm");
      foreach (var a in AppKv.Values.OrderBy(a => a.Id)) {
        var r = a.R;
        WriteLine($"app_{a.Id},{a.InstCount}," +
                  $"{r.Cpu.Max:0.0},{r.Cpu.Avg:0.0},{r.Cpu.Min:0.0},{r.Cpu.Stdev:0.0}," +
                  $"{r.Mem.Max:0.0},{r.Mem.Avg:0.0},{r.Mem.Min:0.0},{r.Mem.Stdev:0.0}," +
                  $"{r.Disk},{r.P},{r.M},{r.Pm}");
      }
    }

    // 这里cpu输出合计最大时刻ts=45的值，mem则是平均值，只使用初赛 DataSet B
    public void PrintInstRequest() {
      WriteLine(AppInstCount);
      foreach (var inst in InitSolution.AppInsts) {
        WriteLine($"{Ceiling(inst.R.Cpu[45])} " +
                  $"{Ceiling(inst.R.Mem.Avg)} " +
                  $"{inst.R.Disk} " +
                  $"inst_{inst.Id}");
      }
    }

    // 分时的集群总体CPU和Mem利用率 
    public void PrintAvgUtilByTs() {
      WriteLine("aid,ts,cpu,mem");
      foreach (var a in AppKv.Values.OrderBy(a => a.Id)) {
        for (var i = 0; i < a.R.Cpu.Length; i++) {
          WriteLine($"{a.Id},{i + 1},{a.R.Cpu[i]},{a.R.Mem[i]}");
        }
      }
    }

    #endregion
  }
}
