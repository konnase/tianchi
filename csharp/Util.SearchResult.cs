using System.Collections.Generic;
using System.IO;

namespace Tianchi {
  public static partial class Program {
    //这里bin是指最终的某个机器放置方案，即要放置到同一台机器的实例列表
    //绝大部分bin在初始状态还没有关联到某台机器
    private static readonly List<List<Instance>> Bins = new List<List<Instance>>(6000);

    /*
    //用于 DataSetA 的代码
    private static int DeployedBinsCount => Bins.Sum(bin => IsDeployed(bin) ? 1 : 0);

    private static void GenDeployDataSetA(string searchResultFile) {
      ParseSearchResult(searchResultFile);

      var bins600 = GetBinsDataSetA(600);
      var bins1024 = GetBinsDataSetA(1024);

      //预先保存这些机器，否则下面分配了空机器后就不好区分了
      var initOccupied600 = OccupiedMachines(600);
      var initOccupied1024 = OccupiedMachines(1024);

      //先把空的机器都占了
      var idx600 = DeployOnIdle(bins600, IdleMachines(600)); // 1724 台
      var idx1024 = DeployOnIdle(bins1024, IdleMachines(1024)); // 1761 台

      //Console.WriteLine($"i600={idx600},i1024={idx1024}");

      //初始occupied的机器中空出来了一些实例
      idx600 = MigrateAndDeploy(idx600, bins600, initOccupied600);
      idx1024 = MigrateAndDeploy(idx1024, bins1024, initOccupied1024);

      Console.WriteLine($"i600={idx600},i1024={idx1024}");
      Console.WriteLine(DeployedBinsCount);

      //期望的最终状态：600的机器共占用2506台，1024的机器共占用3000台
      //Console.WriteLine($"Occupied600={OccupiedMachines(600).Count}," +
      //                  $"Occupied1024={OccupiedMachines(1024).Count}");

      //foreach (var m in Machines) Console.WriteLine(m);
    }

    private static int DeployOnIdle(List<List<Instance>> bins, List<Machine> idleList) {
      var i = 0;
      while (i < idleList.Count && i < bins.Count) {
        DeployBin(idleList[i], bins[i]);
        i++;
      }

      return i;
    }

    private static int MigrateAndDeploy(int binIdx, List<List<Instance>> bins, List<Machine> mList) {
      var mIdx = 0;
      while (binIdx < bins.Count && mIdx < mList.Count) {
        var bin = bins[binIdx++];
        var m = mList[mIdx++];

        if (!DrainMachine(m, bin)) break;
        DeployBin(m, bin);
      }

      return binIdx;
    }

    //将某机器上原有的实例清空，迁移到其它若干机器上去
    private static bool DrainMachine(Machine m, List<Instance> bin) {
      var instList = m.InstList;
      var instCnt = instList.Count;
      var migratedInstCnt = 0;
      //类似Firstfit Desc,将原有实例迁移到若干台机器上去
      for (var i = instList.Count - 1; i >= 0; i--) {
        var inst = instList[i];
        //实例本来就在方案中，不需要迁移，概率很小
        //若跳过此检查，不影响最终成本分数，但会多迁移几次
        if (bin.Contains(inst)) {
          migratedInstCnt++; //计数还是要增加的
          continue;
        }

        foreach (var n in Machines) {
          if (m == n) continue;

          if (n.AddInstance(inst, _w)) {
            migratedInstCnt++;
            break; //FirstFit
          }
        }
      }

      if (migratedInstCnt != instCnt) {
        Console.WriteLine($"Can not migrate existing instances at m_{m.Id}!");
        return false;
      }

      return true;
    }

    private static List<List<Instance>> GetBinsDataSetA(int capDisk) {
      var bins = new List<List<Instance>>(3000);
      var start = 0;
      var end = 3000;
      if (capDisk == 600) {
        //search文件中3000行之后的方案是600 GB硬盘的机器的
        start = 3000;
        end = Bins.Count;
      }

      for (var i = start; i < end; i++) bins.Add(Bins[i]);

      return bins;
    }

    //对（初始状态）占用的机器按大小类型分类，并按cpu.avg排序
    private static List<Machine> OccupiedMachines(int capDisk) {
      return (from m in Machines
        where m.InstList.Count > 0 && m.CapDisk == capDisk
        orderby m.InstList.Sum(i => i.R.Cpu.Avg) //这里顺序很重要！
        select m).ToList();
    }

    private static List<Machine> IdleMachines(int capDisk) {
      return Machines.Where(m => m.IsIdle && m.CapDisk == capDisk).ToList();
    }

    private static void DeployBin(Machine m, List<Instance> bin) {
      foreach (var inst in bin)
        if (!m.AddInstance(inst, _w))
          throw new Exception($"Unkown Error, Deployed Failed!{m}");
    }

    //列表中所有实例是否都部署到了同一台机器上，而且都没有违反亲和性约束    
    private static bool IsDeployed(List<Instance> bin) {
      var m = bin[0].DeployedMachine;
      if (m == null) return false;

      var sameMachine = true;
      foreach (var inst in bin)
        if (inst.DeployedMachine != m || inst.NeedDeployOrMigrate) {
          sameMachine = false;
          break;
        }

      return sameMachine;
    }
    //*/

    //验证装箱搜索结果，需要先读取初始的csv数据
    private static void VerifySearchResult(string searchResultFile) {
      ClearMachineDeployment(); //reset to a clean state
      ParseSearchResult(searchResultFile);

      //Machines.Sort((m, n) => n.CapDisk.CompareTo(m.CapDisk));

      //注意：这里机器类型的排序恰好跟Bins是一致的，故可以共用一个索引变量
      Bins.ForEach((bin, i) => DeployBinNoCheck(Machines[i], Bins[i]));

      PrintScore();
      FinalCheck();
    }

    private static void DeployBinNoCheck(Machine m, List<Instance> bin) {
      bin.ForEach(inst => m.TryPutInst(inst, ignoreCheck: true));
    }

    private static void ParseSearchResult(string searchResultFile) {
      //格式
      //total(0.500000,600): {80,100,80,100,80,80,80} (inst_6297,inst_20827,...)
      var f = File.OpenText(searchResultFile);
      string line;
      while (null != (line = f.ReadLine())) {
        // ReSharper disable once StringIndexOfIsCultureSpecific.1
        var s = line.IndexOf("inst_");
        if (s < 0) continue;

        Bins.Add(line.Substring(s, line.Length - s - 1).CsvToInstList());
      }

      f.Close();
    }
  }
}