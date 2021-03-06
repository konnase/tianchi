using System.Collections.Generic;

namespace Tianchi {
  public static class BinPacking {
    //TODO: SigComm14 Tetris 中按app.R与m.avil.R点积之后 BFD
    //TODO: 从app角度，挑选合适的机器
    //TODO: TPDS00 - An Opportunity Cost （Borg引用的E-PVM）边际成本

    public static void FirstFit(
      IEnumerable<AppInst> appInsts,
      IList<Machine> machines,
      bool forceMigrate = false, // 不管 inst 是否已经部署了，都要迁移
      bool onlyIdleMachine = false // 只部署到空闲机器上
    ) {
      foreach (var inst in appInsts) {
        if (forceMigrate || !inst.IsDeployed) {
          var len = machines.Count;
          for (var i = 0; i < len; i++) {
            var m = machines[i];
            if (inst.Machine == m) {
              continue;
            }

            if (onlyIdleMachine && m.HasApp) {
              continue;
            }

            if (m.TryPut(inst, m.CpuUtilLimit)) {
              break; // First Fit
            }
          }
        }
      }
    }
  }
}
