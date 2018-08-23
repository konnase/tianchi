using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Tianchi {
  public static class DataSetPreA {
    public static readonly DataSet DataSet = Read();

    private static DataSet Read() {
      var ds = DataSet.Read(DataSetId.PreA,
        "data/scheduling_preliminary_app_resources_20180606.csv",
        "data/scheduling_preliminary_machine_resources_20180606.csv",
        "data/scheduling_preliminary_instance_deploy_20180606.csv",
        "data/scheduling_preliminary_app_interference_20180606.csv"
      );

      MachineType.CapDiskLarge = 1024;
      return ds;
    }

    public static void Run(string searchFile = "") {
      var solution = DataSet.DefaultSolution.Clone();
      if (string.IsNullOrEmpty(searchFile)) solution = Fit();

      NaiveSearch.Run(solution, searchFile);
    }

    //搜索结果为：
    //使用的机器数量随cpuUtilLimit单调递减，之后基本稳定
    //但成本先下降，在0.6左右取得最小值，之后缓慢增长
    public static void ScanCpuUtilLimit() {
      var tasks = new List<Task> {
        Task.Run(() => Fit(1.0, 1.0))
      };

      for (var h = 0.56; h < 0.64; h += 0.01)
      for (var l = 0.56; l < 0.64; l += 0.01) {
        var utilH = h;
        var utilL = l;
        var t = Task.Run(() => Fit(utilH, utilL));
        tasks.Add(t);
      }

      foreach (var t in tasks) t.Wait();
    }

    public static void ScanVip() {
      var tasks = new List<Task> {
        Task.Run(() => Fit()), //默认参数
        Task.Run(() => Fit(vipDisk: 250, vipMem: 12, vipCpu: 7)),
        Task.Run(() => Fit(vipDisk: 300, vipMem: 12, vipCpu: 7)),
        Task.Run(() => Fit(vipDisk: 500, vipMem: 12, vipCpu: 7)),
        Task.Run(() => Fit(vipDisk: 300, vipMem: 11, vipCpu: 7)),
        Task.Run(() => Fit(vipDisk: 300, vipMem: 13, vipCpu: 7)),
        Task.Run(() => Fit(vipDisk: 300, vipMem: 12, vipCpu: 6)),
        Task.Run(() => Fit(vipDisk: 300, vipMem: 12, vipCpu: 8))
      };
      foreach (var t in tasks) t.Wait();
    }


    //默认参数使机器数量尽量少，成本分数可以通过搜索降下来
    //对Pre A，不对实例排序结果反而更好
    public static Solution Fit(double cpuUtilH = 0.5, double cpuUtilL = 0.78,
      int vipDisk = 300, int vipMem = 12, int vipCpu = 7) {
      var sol = DataSet.DefaultSolution.Clone();
      var machines = sol.Machines;
      var instances = sol.Instances;

      foreach (var m in machines) m.CpuUtilLimit = m.IsLargeMachine ? cpuUtilH : cpuUtilL;

      var csvSubmit = $"submit_{DataSet.Id}.csv";
      //sol.Writer = File.CreateText(csvSubmit);

      var vips =
        (from inst in instances
          where !inst.Deployed
                && (inst.R.Disk >= vipDisk
                    || inst.R.Mem.Avg >= vipMem
                    || inst.R.Cpu.Avg >= vipCpu)
          select inst).ToList();

      BinPacking.FirstFit(vips, machines, sol.Writer, onlyIdleMachine: true);

      BinPacking.FirstFit(instances, machines, sol.Writer);

      var info = $"=={cpuUtilH:0.00},{cpuUtilL:0.00},{vipDisk},{vipMem},{vipCpu}== ";
      if (!sol.AllInstDeployed) {
        var undeployed = sol.InstCount - sol.DeployedInstCount;
        info += $"{undeployed} instances are not deployed!" +
                $" {sol.DeployedInstCount} of {sol.InstCount} \t e.g. ";
        info += sol.UndeployedInst[0].ToString();
      }

      info += $"\t{sol.ActualScore:0.00},{sol.UsedMachineCount}";

      Console.WriteLine(info);

      if (sol.Writer != null) {
        sol.Writer.Close();
        Console.WriteLine($"== DataSet {DataSet.Id} Judge==");
        sol.JudgeSubmit(csvSubmit);
      }

      return sol;
    }
  }
}