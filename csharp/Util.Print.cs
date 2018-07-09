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

    private static void PrintCsvInitInfo() {
      Console.WriteLine($"App: {AppKv.Count}, " +
                        $"Instance: {Instances.Length}, " +
                        $"Machine: {Machines.Count}");
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
        if (inst.IsDeployed) continue;

        Console.WriteLine(inst);
      }
    }
  }
}