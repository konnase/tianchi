using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    private static int DeployedInstCount => Instances.Sum(inst => inst.NeedDeployOrMigrate ? 0 : 1);

    private static bool AllInstDeployed => Instances.Length == DeployedInstCount;
    private static double TotalCostScore => AllInstDeployed ? Machines.Sum(m => m.Score) : 1e9;
    private static int UsedMachineCount => Machines.Sum(m => m.IsIdle ? 0 : 1);

    private static void ClearMachineDeployment() {
      foreach (var m in Machines) m.ClearInstances();

      Debug.Assert(Instances.All(inst => inst.NeedDeployOrMigrate));
    }

    //private static void ClearMachineDeployment(IEnumerable<Machine> list) {
    //  foreach (var m in list) m.ClearInstances();
    //}

    private static void JudgeSubmit(string csvSubmit, bool verbose = false) {
      if (!File.Exists(csvSubmit)) {
        Console.WriteLine("Error: No Submit File!");
        return;
      }

      ClearMachineDeployment();
      ReadInitDeployment(); //恢复初始状态

      var failedResource = 0;
      var failedX = 0;

      var lineNo = 0;

      ReadCsv(csvSubmit, line => {
        if (failedResource + failedX > 0 && !verbose) {
          return;
        }

        var fields = line.Split(',');
        var instId = fields[0].Id();
        var mId = fields[1].Id();
        var inst = InstanceKv[instId];
        var m = MachineKv[mId];
        lineNo++;
        inst.DeployedMachine?.RemoveInstance(inst); //Debug.Assert(inst.IsInitConflict);

        if (!m.AddInstance(inst)) {
          if (m.IsOverCapacity(inst)) {
            failedResource++;
          }

          if (m.IsXWithDeployed(inst)) {
            failedX++;
          }

          Console.Write($"[{lineNo}] ");
          Console.Write(m.FailedReason(inst));
          Console.WriteLine($"\t{inst}  {m}");
        }
      });

      FinalCheck(verbose);
      PrintScore();
    }

    private static void FinalCheck(bool verbose = false) {
      foreach (var m in Machines) {
        if (m.Avail.Cpu.Min < 0
            || m.Avail.Mem.Min < 0
            || m.Avail.Disk < 0
            || m.Avail.P < 0) {
          Console.Write("Resource over capacity: ");
          Console.WriteLine(m);

          if (!verbose) {
            return;
          }
        }

        var appCnt = m.AppCount;
        foreach (var kv in appCnt) {
          var appB = kv.Key;
          var appBCnt = kv.Value;
          foreach (var ku in appCnt) {
            var appA = ku.Key;
            //因为遍历了两遍app列表，所以这里只需单向检测即可
            if (appBCnt <= appA.XLimit(appB)) continue;
            Console.WriteLine($"Error: [app_{appA.Id},app_{appB.Id}, " +
                              $"{appBCnt} > k={appA.XLimit(appB)}] " +
                              $" @ m_{m.Id}");
            if (!verbose) {
              return;
            }
          }
        }
      }
    }
  }
}