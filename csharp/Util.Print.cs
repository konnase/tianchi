using System;
using System.Collections.Generic;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    private static int DeployedInstCount => Instances.Sum(inst => inst.IsDeployed ? 1 : 0);
    private static bool AllInstDeployed => Instances.Count == DeployedInstCount;
    private static double TotalCostScore => AllInstDeployed ? Machines.Sum(m => m.Score) : 1e9;
    private static int UsedMachineCount => Machines.Sum(m => m.IsIdle ? 0 : 1);

    private static void PrintScore() {
      Console.WriteLine($"TotalScore: " +
                        $"{TotalCostScore:0.00} / {UsedMachineCount}" +
                        $" = [{TotalCostScore / UsedMachineCount:0.00}]");
    }

    private static void PrintCsvInfo() {
      Console.WriteLine($"App: {AppKv.Count}, " +
                        $"Instance: {Instances.Count}, " +
                        $"Machine: {Machines.Count}");
    }

    private static void PrintMachineDeployment(IEnumerable<Machine> machines, bool showInstId = false) {
      var cnt = 1;
      foreach (var m in machines) {
        if (m.IsIdle) continue;

        Console.Write($"[{cnt++}] ");
        Console.Write(m);

        if (showInstId) {
          Console.WriteLine(); //添一个空行
          Console.WriteLine(m.InstListToStr());
        }
      }
    }

    private static void PrintUndeployedInst() {
      foreach (var inst in Instances) {
        if (inst.IsDeployed) continue;

        Console.WriteLine(inst);
      }
    }
  }
}