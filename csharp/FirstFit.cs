using System;
using System.Collections.Generic;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    private static void FirstFit(IEnumerable<Instance> instances, bool onlyIdleMachine = false) {
      foreach (var inst in instances) 
        //对需要放置到空闲机器的vip实例，
        //不管它初始是否已经放置了，都将其移至新的机器上
        if (onlyIdleMachine || inst.NeedDeployOrMigrate)
          foreach (var m in Machines) {
            if (onlyIdleMachine && !m.IsIdle) continue;
            
            // 针对初始部署就违反约束的实例，需将其迁移到其它机器上
            if (inst.DeployedMachine == m) continue;

            if (m.AddInstance(inst, w)) break; //FirstFit
          }
    }

    private static void RunFirstFit() {
      Console.WriteLine("==FirstFit==");

      var vips = from i in Instances
        where i.NeedDeployOrMigrate &&
              (i.R.Disk >= 300
               || i.R.Mem.Avg >= 12
               || i.R.Cpu.Avg >= 7 //5871: 300,12,7
              )
        select i;

      FirstFit(vips, true);

      FirstFit(Instances);

      if (!AllInstDeployed) {
        Console.WriteLine("Failed, Not all instances are depoyed");
        PrintUndeployedInst();
      } else {
        PrintScore();
      }
    }
  }
}