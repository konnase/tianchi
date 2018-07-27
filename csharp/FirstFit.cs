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

            //if (m.AddInstance(inst, _w)) break; //FirstFit
            if (m.AddInstance(inst)) break; //FirstFit
          }
    }

    private static void RunFirstFit() {
      Console.WriteLine("==FirstFit==");

//      var vips = from i in Instances
//        where i.NeedDeployOrMigrate &&
//              (i.R.Disk >= 300
//               || i.R.Mem.Avg >= 12
//               || i.R.Cpu.Avg >= 7 //5871: 300,12,7
//              )
//        select i;
//
//      FirstFit(vips, true);

      var overLoaded = Machines.Where(m => m.Score > 3);
      foreach (var m in overLoaded) {
        m.ClearInstances();
      }

      var instList = Instances.OrderByDescending(inst => inst.R.Cpu.Avg); //Cpu[45]

      FirstFit(instList);

      var outlier = Instances.Where(i => i.NeedDeployOrMigrate);
      Console.WriteLine(outlier.ToList()[0]);

      if (!AllInstDeployed) {
        Console.WriteLine("Failed, Not all instances are depoyed");
        Console.WriteLine($"{DeployedInstCount}/{InstCount}");
        //PrintUndeployedInst();
      } else
        PrintScore();
    }


    //TODO: SigComm14 Tetris 中按app.R与m.avil.R点积之后 BFD
    //TODO: [X] 从机器的角度，挑选合适的app; [Y] 还是从app角度，挑选合适的机器？
  }
}