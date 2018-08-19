using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Tianchi {
  public static class DataSetPreB {
    public static readonly DataSet DataSet = Read();

    private static DataSet Read() {
      var ds = DataSet.Read(DataSetId.PreB,
        "data/scheduling_preliminary_b_app_resources_20180726.csv",
        "data/scheduling_preliminary_b_machine_resources_20180726.csv",
        "data/scheduling_preliminary_b_instance_deploy_20180726.csv",
        "data/scheduling_preliminary_b_app_interference_20180726.csv"
      );

      MachineType.LargeDiskCap = 2457;

      return ds;
    }

    public static void Run(string searchFile = "") {
      var sol = DataSet.DefaultSolution.Clone();
      if (string.IsNullOrEmpty(searchFile)) sol = Fit();

      LocalSearch.Run(sol, searchFile);
    }

    //搜索结果为：
    //使用的机器数量随cpuUtilLimit单调递减，之后基本稳定
    //但成本先下降，在0.6左右取得最小值，之后缓慢增长
    public static void ScanCpuUtilLimit() {
      var tasks = new List<Task>();

      for (var h = 0.65; h < 0.75; h += 0.02)
      for (var l = 0.65; l < 0.75; l += 0.02) {
        var utilH = h;
        var utilL = l;
        var t = Task.Run(() => Fit(utilH, utilL));
        tasks.Add(t);
      }

      foreach (var t in tasks) t.Wait();
    }

    public static void ScanHighCpu(IList<Machine> machines, double start, double end, double step = 0.01) {
      for (var th = start; th < end; th += step) {
        var highCpuUtilList = HighCpuUtilInsts(machines, th);
        Console.WriteLine($"== {th:0.00}: {highCpuUtilList.Count}");
      }
    }

    public static List<Instance> HighCpuUtilInsts(IList<Machine> machines, double threshold) {
      var instList = new List<Instance>(3000);
      var u = new Series(Resource.TsCount);

      foreach (var m in machines) {
        if (m.IsIdle) continue;

        var cap = m.CapCpu * threshold;
        var usageCpu = m.Usage.Cpu;

        if (usageCpu.Max > cap) {
          u.CopyFrom(usageCpu);
          var insts = m.InstSet.ToList().OrderByDescending(inst => inst.R.Cpu.Max);
          foreach (var i in insts) {
            u.Subtract(i.R.Cpu);
            if (u.Max < cap) break;

            instList.Add(i); //将 inst 加入待迁移列表，需 ForceMigrate
          }
        }
      }

      return instList;
    }

    public static Solution Fit(double cpuUtilH = 0.65, double cpuUtilL = 0.65) {
      var sol = DataSet.DefaultSolution.Clone();
      var machines = sol.Machines;
      var instances = sol.Instances;

      foreach (var m in machines) m.CpuUtilLimit = m.IsLargeMachine ? cpuUtilH : cpuUtilL;

      var csvSubmit = $"submit_{DataSet.Id}.csv";
      //sol.Writer = File.CreateText(csvSubmit);

      var instList = HighCpuUtilInsts(machines, Math.Min(cpuUtilH, cpuUtilL)); //threshold可以tuning
      BinPacking.FirstFit(instList, machines, forceMigrate: true);

      BinPacking.FirstFit(instances, machines, sol.Writer);

      var info = $"=={cpuUtilH:0.00},{cpuUtilL:0.00}== ";
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