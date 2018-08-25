using System.Collections.Generic;
using System.Linq;
using TPL = System.Threading.Tasks;
using static System.Console;
using static System.Math;

namespace Tianchi {
  public static class DataSetPreB {
    public static readonly DataSet DataSet;
    public static readonly Solution InitSolution;

    static DataSetPreB() {
      MachineType.CapDiskLarge = 2457;

      DataSet = DataSet.Read(DataSetId.PreB,
        "data/scheduling_preliminary_b_app_resources_20180726.csv",
        "data/scheduling_preliminary_b_app_interference_20180726.csv",
        "data/scheduling_preliminary_b_machine_resources_20180726.csv",
        "data/scheduling_preliminary_b_instance_deploy_20180726.csv",
        isAlpha10: true
      );

      InitSolution = DataSet.InitSolution;
    }

    //TODO: 把python data set b的ffd移植过来
    //TODO: 对机器排序？

    //搜索结果为：
    //使用的机器数量随cpuUtilLimit单调递减，之后基本稳定
    //但成本先下降，在0.6左右取得最小值，之后缓慢增长
    public static void ScanCpuUtilLimit() {
      var tasks = new List<TPL.Task>();

      for (var h = 0.65; h < 0.75; h += 0.02)
      for (var l = 0.65; l < 0.75; l += 0.02) {
        var utilH = h;
        var utilL = l;
        var t = TPL.Task.Run(() => Fit(utilH, utilL));
        tasks.Add(t);
      }

      foreach (var t in tasks) t.Wait();
    }

    public static void ScanHighCpu(IList<Machine> machines, double start, double end,
      double step = 0.01) {
      for (var th = start; th < end; th += step) {
        var highCpuUtilList = HighCpuUtilAppInsts(machines, th);
        WriteLine($"== {th:0.00}: {highCpuUtilList.Count}");
      }
    }

    // threshold 也可以调参
    public static List<AppInst> HighCpuUtilAppInsts(IList<Machine> machines, double threshold) {
      var instList = new List<AppInst>(3000);
      var u = new Series(Resource.Ts1470);

      foreach (var m in machines) {
        if (m.IsIdle) continue;

        var th = m.CapCpu * threshold;
        var usageCpu = m.Usage.Cpu;

        if (usageCpu.Max < th) continue;

        u.CopyFrom(usageCpu);
        var insts = m.AppInstSet.ToList().OrderByDescending(inst => inst.R.Cpu.Max);
        foreach (var i in insts) {
          u.Subtract(i.R.Cpu);
          if (u.Max < th) break;

          instList.Add(i); // 将 inst 加入待迁移列表，需要 ForceMigrate
        }
      }

      return instList;
    }

    public static Solution Fit(double cpuUtilH = 0.65, double cpuUtilL = 0.65,
      bool saveSubmitCsv = false) {
      var sol = DataSet.InitSolution.Clone();
      var machines = sol.Machines;
      var appInsts = sol.AppInsts;

      foreach (var m in machines) m.CpuUtilLimit = m.IsLargeMachine ? cpuUtilH : cpuUtilL;

      var instList = HighCpuUtilAppInsts(machines, Min(cpuUtilH, cpuUtilL));
      BinPacking.FirstFit(instList, machines, true);

      BinPacking.FirstFit(appInsts, machines);

      var msg = $"==Fit@{cpuUtilH:0.00},{cpuUtilL:0.00}== ";
      if (!sol.AppInstAllDeployed) {
        var undeployed = sol.AppInstCount - sol.AppInstDeployedCount;
        msg += $"undeployed: {undeployed} = " +
               $" {sol.AppInstCount} - {sol.AppInstDeployedCount}\t e.g. ";
        msg += sol.AppInstUndeployed[0].ToString();
      }

      msg += $"\t{sol.ActualScore:0.00},{sol.UsedMachineCount}";

      WriteLine(msg);

      if (saveSubmitCsv) Solution.SaveAndJudgeApp(sol);

      return sol;
    }
  }
}
