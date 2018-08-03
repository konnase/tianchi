using System;
using System.Collections.Generic;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    private static void PrintScore() {
      Console.WriteLine($"TotalScore: " +
                        $"{TotalScore:0.00} / {UsedMachineCount}" +
                        $" = [{TotalScore / UsedMachineCount:0.00}]");
    }

    private static void PrintUndeployedInst() {
      foreach (var inst in Instances)
        if (inst.NeedDeployOrMigrate)
          Console.WriteLine(inst);
    }

    private static void PrintCsvInitInfo() {
      Console.WriteLine("==Init==");
      Console.WriteLine($"App#: {AppKv.Count}, " +
                        $"Instance#: {Instances.Length}, " +
                        $"Machine#: {Machines.Count}");

      //以下输出初始数据中违反约束的情况
      //FinalCheck(verbose:true); //DataSetA: 151

      //初始数据中，忽略了约束，被强制部署到机器上的实例
      var xInstList = Instances
        .Where(i => i.Machine != null && i.NeedDeployOrMigrate)
        .ToList();

      if (xInstList.Count > 0)
        Console.WriteLine($"xInstList.Count: {xInstList.Count}"); //DataSetA: 143

      var xMachineList = Machines
        .Where(m => !m.IsIdle && m.InstList.Any(i => i.NeedDeployOrMigrate))
        .ToList();

      if (xMachineList.Count > 0) Console.WriteLine($"xMachineList.Count: {xMachineList.Count}");

      //DataSetA
      //注意：与上述FinalCheck结果不同，但这是合理的；
      //一台机器上可能有多个实例发生冲突，导致计数出入。
      //共117台机器上存在约束冲突
      foreach (var m in xMachineList)
      foreach (var x in m.ConflictList)
        Console.WriteLine($"m_{m.Id},A:app_{x.Item1.Id},B:app_{x.Item2.Id}," +
                          $"BCnt:{x.Item3} > BLimit:{x.Item4}");
    }

    //DataSetB
    //注意：这里cpu输出合计最大时刻ts=45的值，mem则是平均值
    private static void PrintInstRequest() {
      Console.WriteLine(InstCount);
      foreach (var inst in Instances)
        Console.WriteLine($"{Math.Ceiling(inst.R.Cpu[45])} " +
                          $"{Math.Ceiling(inst.R.Mem.Avg)} " +
                          $"{inst.R.Disk} " +
                          $"inst_{inst.Id}");
    }

    //输出有实例部署的机器资源占用情况
    private static void PrintMachine(IEnumerable<Machine> machines) {
      Console.WriteLine("cap_disk,mid,score," +
                        "avl_cpu_min,util_cpu_max,util_cpu_avg," +
                        "avl_mem_min,util_mem_max,util_mem_avg," +
                        "avl_disk,util_disk,avl_p," +
                        "inst_cnt,inst_list");
      machines.ForEach(Console.WriteLine);
    }

    private static void PrintAppUtilStat() {
      Console.WriteLine("aid,inst_cnt," +
                        "cpu_max,cpu_avg,cpu_min,cpu_stdev," +
                        "mem_max,mem_avg,mem_min,mem_stedv," +
                        "disk,p,m,pm");

      foreach (var a in AppKv.Values.OrderBy(a => a.Id)) {
        var r = a.R;
        Console.WriteLine($"app_{a.Id},{a.InstanceCount}," +
                          $"{r.Cpu.Max:0.0},{r.Cpu.Avg:0.0}," +
                          $"{r.Cpu.Min:0.0},{r.Cpu.Stdev:0.0}," +
                          $"{r.Mem.Max:0.0},{r.Mem.Avg:0.0}," +
                          $"{r.Mem.Min:0.0},{r.Mem.Stdev:0.0}," +
                          $"{r.Disk},{r.P},{r.M},{r.Pm}");
      }
    }

    private static void PrintRequestUtil() {
      var r = new Resource();

      foreach (var inst in Instances) r.Add(inst.R);

      var totalCapCpu = Machines.Sum(m => m.CapCpu);
      var totalCapMem = Machines.Sum(m => m.CapMem);
      var totalCapDisk = Machines.Sum(m => m.CapDisk);
      var totalCapP = Machines.Sum(m => m.Capacity.P);
      var totalCapM = Machines.Sum(m => m.Capacity.M);
      var totalCapPm = Machines.Sum(m => m.Capacity.Pm);

      Console.WriteLine($"Disk: {r.Disk},{r.Disk * 100.0 / totalCapDisk:0.00}%");
      Console.WriteLine($"P: {r.P},{r.P * 100.0 / totalCapP:0.00}%");
      Console.WriteLine($"M: {r.M},{r.M * 100.0 / totalCapM:0.00}%");
      Console.WriteLine($"PM: {r.Pm},{r.Pm * 100.0 / totalCapPm:0.00}%");

      Console.WriteLine();
      Console.WriteLine("ts,cpu,mem,util_cpu,util_mem");
      for (var i = 0; i < Resource.TsCount; i++)
        Console.WriteLine($"{i + 1},{r.Cpu[i]:0.0},{r.Mem[i]:0.0}," +
                          $"{r.Cpu[i] * 100 / totalCapCpu:0.000}%," +
                          $"{r.Mem[i] * 100 / totalCapMem:0.000}%");
    }

    private static void PrintSearch() {
      //格式
      //total(0.204543,1020):
      //{650,60,60,40,150,60}
      //(inst_68485,inst_96520,inst_8627,inst_69755,inst_84710,inst_56168)
      var list = from m in Machines
        where !m.IsIdle
        orderby m.CapDisk descending,
          m.CapDisk - m.Avail.Disk descending, // used disk
          m.Score descending,
          m.InstList.Count
        select m;

      foreach (var m in list) Console.WriteLine(m.ToSearchStr());
    }

    private static void PrintInitStats() {
      PrintCsvInitInfo();
      Console.WriteLine();
      PrintRequestUtil();
      //Console.WriteLine();
      //PrintAppUtilStat();
      //Console.WriteLine();
      //PrintMachine(Machines.Where(m => !m.IsIdle));
      //Console.WriteLine();
      //PrintUtilTs();
    }

    //将CPU和Mem分时数据转置一下
    /*
    private static void PrintUtilTs() {
      Console.WriteLine($"aid,ts,cpu,mem");
      foreach (var a in AppKv.Values.OrderBy(a => a.Id)) {
        for (var i = 0; i < Resource.TsCount; i++) {
          Console.WriteLine($"{a.Id},{i + 1},{a.R.Cpu[i]},{a.R.Mem[i]}");
        }
      }
    } //*/
  }
}