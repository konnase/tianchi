using System.Collections.Generic;
using System.Linq;
using TPL = System.Threading.Tasks;
using static System.Console;

namespace Tianchi {
  public static class DataSetPreA {
    public static readonly DataSet DataSet;
    public static readonly Solution InitSolution;

    static DataSetPreA() {
      MachineType.CapDiskLarge = 1024;

      DataSet = DataSet.Read(DataSetId.PreA,
        "data/scheduling_preliminary_app_resources_20180606.csv",
        "data/scheduling_preliminary_app_interference_20180606.csv",
        "data/scheduling_preliminary_machine_resources_20180606.csv",
        "data/scheduling_preliminary_instance_deploy_20180606.csv",
        isAlpha10: true
      );

      InitSolution = DataSet.InitSolution;
    }

    //搜索结果为：
    //使用的机器数量随cpuUtilLimit单调递减，之后基本稳定
    //但成本先下降，在0.6左右取得最小值，之后缓慢增长
    public static void ScanCpuUtilLimit() {
      var tasks = new List<TPL.Task> {TPL.Task.Run(() => Fit(1.0, 1.0))};

      for (var h = 0.56; h < 0.64; h += 0.01)
      for (var l = 0.56; l < 0.64; l += 0.01) {
        var utilH = h;
        var utilL = l;
        var t = TPL.Task.Run(() => Fit(utilH, utilL));
        tasks.Add(t);
      }

      foreach (var t in tasks) t.Wait();
    }

    public static void ScanVip() {
      var tasks = new List<TPL.Task> {
        TPL.Task.Run(() => Fit()), //默认参数
        //TPL.Task.Run(() => Fit(vipDisk: 250, vipMem: 12, vipCpu: 7)),
        //TPL.Task.Run(() => Fit(vipDisk: 300, vipMem: 12, vipCpu: 7)),
        //TPL.Task.Run(() => Fit(vipDisk: 500, vipMem: 12, vipCpu: 7)),
        //TPL.Task.Run(() => Fit(vipDisk: 300, vipMem: 11, vipCpu: 7)),
        //TPL.Task.Run(() => Fit(vipDisk: 300, vipMem: 13, vipCpu: 7)),
        //TPL.Task.Run(() => Fit(vipDisk: 300, vipMem: 12, vipCpu: 6)),
        TPL.Task.Run(() => Fit(vipDisk: 300, vipMem: 12, vipCpu: 8))
      };
      foreach (var t in tasks) t.Wait();
    }


    //默认参数使机器数量尽量少，成本分数可以通过搜索降下来
    //对Pre A，不对实例排序结果反而更好
    public static Solution Fit(double cpuUtilH = 0.5, double cpuUtilL = 0.78,
      int vipDisk = 300, int vipMem = 12, int vipCpu = 7, bool saveSubmitCsv = false) {
      //
      var sol = InitSolution.Clone();
      var machines = sol.Machines;
      var appInsts = sol.AppInsts;

      foreach (var m in machines) m.CpuUtilLimit = m.IsLargeMachine ? cpuUtilH : cpuUtilL;

      var vips = (from inst in appInsts
        where !inst.Deployed && (inst.R.Disk >= vipDisk
                                 || inst.R.Mem.Avg >= vipMem
                                 || inst.R.Cpu.Avg >= vipCpu)
        select inst).ToList();

      BinPacking.FirstFit(vips, machines, onlyIdleMachine: true);

      BinPacking.FirstFit(appInsts, machines);

      var msg = $"==Fit@{cpuUtilH:0.00},{cpuUtilL:0.00},{vipDisk},{vipMem},{vipCpu}== ";
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
