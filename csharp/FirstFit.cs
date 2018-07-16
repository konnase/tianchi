using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
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
  }
}