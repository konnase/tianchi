using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    private static string _projectPath = "D:/tianchi/";

    //这里使用固定的文件名，覆盖旧数据
    private static string CsvSubmit => $"{_projectPath}/submit.csv";
    //submit_{DateTime.Now:yyyyMMdd_hhmmss}.csv";

    // ReSharper disable once ParameterTypeCanBeEnumerable.Local
    private static void FirstFit(IEnumerable<Instance> instances, bool onlyIdleMachine = false) {
      foreach (var inst in instances) {
        if (inst.IsDeployed) continue;

        foreach (var m in Machines) {
          if (onlyIdleMachine && !m.IsIdle) continue;

          if (m.AddInstance(inst)) break;
        }
      }
    }

    private static void RunFFD() {
      Console.WriteLine("==Deploy==");

      var vip1Insts = from i in Instances
        where !i.IsDeployed &&
              (i.R.Disk >= 500
               || i.R.Mem.Avg >= 15
               || i.R.Cpu.Avg >= 6)
        select i;

      FirstFit(vip1Insts, onlyIdleMachine: true);

      var vip2Insts = from i in Instances
        where !i.IsDeployed && i.R.Disk >= 100
        select i;

      FirstFit(vip2Insts);

      Instances.Sort((a, b) => 0);

      FirstFit(Instances);
      var outlierMachines = from m in Machines
        where m.Score > 1.5
        // Math.Abs(0.5 - m.UtilCpuAvg) > 0.3  ||
        // 1.0 - m.UtilMemAvg > 0.3 ||
        // 1.0 - m.UtilDisk > 0.3
        select m;

      // PrintMachineDeployment(outlierMachines);

      if (!AllInstDeployed) {
        PrintUndeployedInst();
      } else {
        PrintScore();
        WriteSubmitCsv(CsvSubmit);

        Console.WriteLine("==Verify==");
        ClearMachineDeployment();
        ReadInitDeployment(); //恢复初始状态

        VerifySubmit(CsvSubmit);
      }
    }

    private static void VerifySubmit(string csvSubmit) {
      if (!File.Exists(csvSubmit)) {
        Console.WriteLine("Error: No Submit File!");
        return;
      }

      ReadCsv(csvSubmit, line => {
        var fields = line.Split(',');
        var instId = fields[0].Id();
        var mId = fields[1].Id();
        var inst = InstanceKv[instId];
        var m = MachineKv[mId];

        if (!m.AddInstance(inst)) {
          Console.Write(m.FailedReason(inst));
          Console.Write("\t");
          Console.WriteLine(m.ToString());
        }
      });

      PrintScore();
    }

    private static void Main(string[] args) {
      if (args.Length == 1) {
        _projectPath = args[0];
      }

      ReadAllData();
      //Console.WriteLine("==Init==");
      //PrintCsvInfo();

      ParseOut("D:/tianchi/out-n169");

      Console.WriteLine("==End==");
    }
  }
}