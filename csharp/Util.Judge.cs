using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    private static int DeployedInstCount => Instances.Sum(inst => inst.NeedDeployOrMigrate ? 0 : 1);

    private static bool AllInstDeployed => Instances.Length == DeployedInstCount;
    private static double TotalScore => AllInstDeployed ? ActualScore : 1e9;
    private static double ActualScore => Machines.Sum(m => m.Score);
    private static int UsedMachineCount => Machines.Sum(m => m.IsIdle ? 0 : 1);

    private static void ClearMachineDeployment() {
      Machines.ForEach(m => m.ClearAllInsts());

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
      ReadInitDeployment(CsvDeploy); //恢复初始状态

      ReadSubmit(csvSubmit, verbose);

      PrintScore();
      FinalCheck(verbose);
    }

    private static void ReadSubmit(string csvSubmit, bool verbose = false) {
      _w?.Close();

      var failedCntResource = 0;
      var failedCntX = 0;

      var lineNo = 0;

      ReadCsv(csvSubmit, fields => {
        if (failedCntResource + failedCntX > 0 && !verbose) return;
        var instId = fields[0].Id();
        var mId = fields[1].Id();
        var inst = InstanceKv[instId];
        var m = MachineKv[mId];
        lineNo++;
        inst.Machine?.RemoveInst(inst);

        if (!m.TryPutInst(inst)) {
          if (m.IsOverCapWithInst(inst)) failedCntResource++;

          if (m.HasConflictWithInst(inst)) failedCntX++;

          Console.Write($"[{lineNo}] ");
          Console.Write(m.FailedReason(inst));
          Console.WriteLine($"\t{inst}  {m}");
        }
      });
    }

    private static bool FinalCheck(bool verbose = false) {
      var ok = true;
      if (!AllInstDeployed) {
        Console.WriteLine($"Deployed insts {DeployedInstCount} of [{InstCount}]");
        return false;
      }

      foreach (var m in Machines) {
        if (m.IsOverCapacity) {
          Console.WriteLine(m);
          ok = false;
          if (!verbose) return false;
        }

        foreach (var x in m.ConflictList) {
          Console.WriteLine($"m_{m.Id}," +
                            $"[app_{x.Item1.Id},app_{x.Item2.Id}," +
                            $"{x.Item3} > k={x.Item4}]");
          ok = false;
          if (!verbose) return false;
        }
      }

      return ok;
    }
  }
}