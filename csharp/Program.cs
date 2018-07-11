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

    private static StreamWriter _writer;

    // ReSharper disable once ParameterTypeCanBeEnumerable.Local
    private static void FirstFit(IEnumerable<Instance> instances, bool onlyIdleMachine = false) {
      foreach (var inst in instances) {
        if (onlyIdleMachine || inst.NeedDeployOrMigrate) {
          foreach (var m in Machines) {
            if (onlyIdleMachine && !m.IsIdle) continue;

            if (m.AddInstance(inst, _writer)) break;
          }
        }
      }
    }

    private static void RunFf() {
      Console.WriteLine("==Deploy==");

      _writer = File.CreateText(CsvSubmit);

      var vips = from i in Instances
        where i.NeedDeployOrMigrate &&
              (i.R.Disk >= 300
               || i.R.Mem.Avg >= 12
               || i.R.Cpu.Avg >= 7 //5871: 300,12,7
              )
        select i;

      FirstFit(vips, onlyIdleMachine: true);

      FirstFit(Instances);

      if (!AllInstDeployed) {
        PrintUndeployedInst();
      } else {
        PrintScore();
      }

      _writer.Close();
    }

    private static void Main(string[] args) {
      if (args.Length == 1) {
        _projectPath = args[0];
      }

      ReadAllData();
      //Console.WriteLine("==Init==");
      //PrintCsvInfo();

      RunFf();

      Console.WriteLine("==Judge==");
      JudgeSubmit(CsvSubmit);

      //var outlierMachines = from m in Machines
      //  where m.Score > 1.5 ||
      //        Math.Abs(0.5 - m.UtilCpuAvg) > 0.3 ||
      //        1.0 - m.UtilMemAvg > 0.3 ||
      //        1.0 - m.UtilDisk > 0.3
      //  select m;

      // PrintMachineDeployment(outlierMachines);

      //ParseOut($"{_projectPath}/out-n169");

      Console.WriteLine("==End==");
    }
  }
}