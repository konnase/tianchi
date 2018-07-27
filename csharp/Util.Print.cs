using System;
using System.Collections.Generic;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    private static void PrintScore() {
      Console.WriteLine($"TotalScore: " +
                        $"{TotalCostScore:0.00} / {UsedMachineCount}" +
                        $" = [{TotalCostScore / UsedMachineCount:0.00}]");
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

      //FinalCheck(true); //DataSetA: 151;
      var xInstList = (from i in Instances
        where i.DeployedMachine != null && i.NeedDeployOrMigrate
        select i).ToList(); //初始被忽略约束，强制部署到机器上的实例

      if (xInstList.Count > 0)
        Console.WriteLine(xInstList.Count); //DataSetA: 143; 没有资源超限的初始部署实例；

      //注意：与上述FinalCheck结果不同，但这是合理的；
      //一台机器上可能有多个实例发生冲突，导致计数出入。
      //共117台机器上存在约束冲突
      foreach (var inst in xInstList)
        //inst可能存在多个冲突，这里不再一一找出，只输出机器上的应用计数列表
        Console.WriteLine($"m_{inst.DeployedMachine.Id},app_{inst.App.Id}," +
                          $"inst_{inst.Id},[{inst.DeployedMachine.AppListToStr()}]");
    }

    //注意：这里cpu输出合计最大时刻的值，mem则是平均值
    private static void PrintInstRequest() {
      Console.WriteLine(InstCount);
      foreach (var inst in Instances)
        Console.WriteLine($"{Math.Ceiling(inst.R.Cpu[45])} {Math.Ceiling(inst.R.Mem.Avg)} " +
                          $"{inst.R.Disk} inst_{inst.Id}");
    }

    //输出有实例部署的机器资源占用情况
    private static void PrintMachine(IEnumerable<Machine> machines) {
      Console.WriteLine("cap_disk,mid,score," +
                        "avl_cpu_min,util_cpu_max,util_cpu_avg," +
                        "avl_mem_min,util_mem_max,util_mem_avg," +
                        "avl_disk,util_disk,avl_p," +
                        "inst_cnt,inst_list");
      machines.Each(Console.WriteLine);
    }

    private static void PrintAppUtilStat() {
      var r = new Resource();
      Console.WriteLine("aid,inst_cnt," +
                        "cpu_max,cpu_avg,cpu_min,cpu_stdev," +
                        "mem_max,mem_avg,mem_min,mem_stedv," +
                        "disk,p,m,pm");

      foreach (var a in AppKv.Values.OrderBy(a => a.Id))
        Console.WriteLine($"app_{a.Id},{a.InstanceCount}," +
                          $"{a.R.Cpu.Max:0.0},{a.R.Cpu.Avg:0.0}," +
                          $"{a.R.Cpu.Min:0.0},{a.R.Cpu.Stdev:0.0}," +
                          $"{a.R.Mem.Max:0.0},{a.R.Mem.Avg:0.0}," +
                          $"{a.R.Mem.Min:0.0},{a.R.Mem.Stdev:0.0}," +
                          $"{a.R.Disk},{a.R.P},{a.R.M},{a.R.Pm}");
    }

    private static void PrintRequestUtil() {
      var r = new Resource();

      foreach (var inst in Instances) r.Add(inst.R);

      var totalCpuCap = Machines.Sum(m => m.CapCpu);
      var totalMemCap = Machines.Sum(m => m.CapMem);
      var totalDiskCap = Machines.Sum(m => m.CapDisk);
      var totalPCap = Machines.Sum(m => m.Cap.P);
      var totalMCap = Machines.Sum(m => m.Cap.M);
      var totalPmCap = Machines.Sum(m => m.Cap.Pm);

      Console.WriteLine($"Disk: {r.Disk},{r.Disk * 100.0 / totalDiskCap:0.00}%");
      Console.WriteLine($"P: {r.P},{r.P * 100.0 / totalPCap:0.00}%");
      Console.WriteLine($"M: {r.M},{r.M * 100.0 / totalMCap:0.00}%");
      Console.WriteLine($"PM: {r.Pm},{r.Pm * 100.0 / totalPmCap:0.00}%");

      Console.WriteLine();
      Console.WriteLine("ts,cpu,mem,util_cpu,util_mem");
      for (var i = 0; i < Resource.TsCount; i++)
        Console.WriteLine($"{i + 1},{r.Cpu[i]:0.0},{r.Mem[i]:0.0}," +
                          $"{r.Cpu[i] * 100 / totalCpuCap:0.000}%," +
                          $"{r.Mem[i] * 100 / totalMemCap:0.000}%");
    }

    private static void PrintSearch() {
      //格式
      //total(0.204543,1020):
      //{650,60,60,40,150,60}
      //(inst_68485,inst_96520,inst_8627,inst_69755,inst_84710,inst_56168)
      var list = from m in Machines
        orderby m.CapDisk descending,
          m.CapDisk - m.Avail.Disk descending, // used disk
          m.Score descending,
          m.InstList.Count
        select m;

      foreach (var m in list) {
        if (m.IsIdle) continue;
        m.InstList.Sort((i, j) => j.R.Disk.CompareTo(i.R.Disk));
        Console.WriteLine($"total({m.Score:0.00},{m.CapDisk - m.Avail.Disk}): " +
                          $"{{{m.InstList.ToStr(i => i.R.Disk)}}} " +
                          $"({m.InstListToStr()})");
      }
    }

    private static void PrintInitStats() {
      PrintCsvInitInfo();
      Console.WriteLine();
      PrintRequestUtil();
      Console.WriteLine();
      PrintAppUtilStat();
      Console.WriteLine();
      PrintMachine(Machines.Where(m => !m.IsIdle));
      Console.WriteLine();
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