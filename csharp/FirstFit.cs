using System;
using System.Collections.Generic;

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
            if (inst.Machine == m) continue;

            if (m.TryPutInst(inst, _w)) break; //FirstFit
          }
    }

    private static void RunFirstFit() {
      ReadAllData(DataSet);
      //_w = File.CreateText(CsvSubmit);

      Console.WriteLine("==FirstFit==");

      //var instList = Instances.OrderBy(inst => inst.R.Cpu.Avg); //Cpu[45]
      FirstFit(Instances);

      if (!AllInstDeployed) {
        Console.WriteLine("Failed, Not all instances are depoyed");
        Console.WriteLine($"{DeployedInstCount}/{InstCount}");
        //PrintUndeployedInst();
      } else {
        PrintScore();
      }

      _w?.Close();
      //Console.WriteLine("==Judge==");
      //JudgeSubmit(CsvSubmit);
    }

    //TODO: SigComm14 Tetris 中按app.R与m.avil.R点积之后 BFD
    //TODO: [X] 从机器的角度，挑选合适的app; [Y] 还是从app角度，挑选合适的机器？
    //TODO: TPDS00 - An Opportunity Cost （Borg引用的E-PVM）编辑成本
  }
}