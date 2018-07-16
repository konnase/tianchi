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

    private static void PrintMachineDeployment(IEnumerable<Machine> machines, bool showInstId = false) {
      var cnt = 1;
      foreach (var m in machines) {
        if (m.IsIdle) continue;

        Console.Write($"[{cnt++}] ");
        Console.Write(m);

        if (showInstId) {
          Console.WriteLine(m.InstListToStr());
          Console.WriteLine();
        }
      }
    }

    private static void PrintUndeployedInst() {
      foreach (var inst in Instances) {
        if (inst.NeedDeployOrMigrate) {
          Console.WriteLine(inst);
        }
      }
    }

    private static void PrintCsvInitInfo() {
      Console.WriteLine("==Init==");
      Console.WriteLine($"App: {AppKv.Count}, " +
                        $"Instance: {Instances.Length}, " +
                        $"Machine: {Machines.Count}");

      FinalCheck(verbose: true); //151;
      var insts = (from i in Instances
        where i.DeployedMachine != null && i.NeedDeployOrMigrate
        select i).ToList(); //初始被忽略约束，强制部署到机器上的实例
      Console.WriteLine(insts.Count()); //143; 没有资源超限的初始部署实例；
                                        //注意，与上述FinalCheck结果不同，但这是合理的
      foreach (var i in insts) {
        Console.WriteLine($"m_{i.DeployedMachine.Id},app_{i.App.Id}," +
                          $"inst_{i.Id},{i.DeployedMachine.AppListToStr()}");
      }
    }
  }
}