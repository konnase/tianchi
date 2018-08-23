//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Linq;
//
//namespace Tianchi {
//  public static class SearchResult {
//    private static DataSet _dataSet;
//    private static Solution _solution;
//    private static Machine[] _machines;
//    private static StreamWriter _w;
//
//    //这里bin是指最终的某个机器放置方案，即要放置到同一台机器的实例列表
//    //绝大部分bin在初始状态还没有关联到某台机器
//    private static List<List<Instance>> _bins;
//
//    private static int DeployedBinsCount => _bins.Sum(bin => IsDeployed(bin) ? 1 : 0);
//
//    private static void RunSearchResult(DataSet dataSet, string searchResultFile) {
//      //searchResultFile = "search-result/search_6097_83_4834m";
//      if (!File.Exists(searchResultFile)) {
//        Console.Error.WriteLine($"Error: Cannot find search file {searchResultFile}");
//        Environment.Exit(-1);
//      }
//
//      _dataSet = dataSet;
//      _solution = _dataSet.DefaultSolution;
//      _machines = _solution.Machines;
//
//      _bins = new List<List<Instance>>(_dataSet.MachineCount);
//      ParseSearchResult(searchResultFile);
//
//      //*************************************************************************
//      //仅验证search file在没有初始部署情况下的正确性
//      Verify(searchResultFile);
//
//      //保存到项目根目录
//      var submitCsv = $"submit_{dataSet.Id.ToStr()}_{_solution.ActualScore:0}.csv";
//      _w = File.CreateText(submitCsv);
//
//      switch (dataSet.Id) {
//        case DataSetId.PreA:
//          ToSubmitDataSetA(); //从search file 生成 submit.csv
//          break;
//        case DataSetId.PreB:
//          ToSubmitDataSetB();
//          break;
//        default:
//          throw new ArgumentOutOfRangeException();
//      }
//    }
//
//    //验证搜索结果，需要先读取初始的csv数据
//    public static void Verify(string searchFile) {
//      _solution.ClearDeploy(); //reset to a clean state
//
//      //注意：这里机器类型的排序恰好跟Bins是一致的，故可以共用一个索引变量
//      _bins.ForEach((bin, i) =>
//        bin.ForEach(inst => _machines[i].TryPutInst(inst, ignoreCheck: true)));
//
//      _solution.PrintScore();
//      _solution.FinalCheck();
//    }
//
//
//    //初赛数据集A的搜索结果是从空白状态开始的，
//    //而数据集B是从FFD结果开始的，所以B的格式转换比较简单
//    //*************************************************************************
//    //用于 DataSetB 的代码
//    private static void ToSubmitDataSetB() {
//      MigrateAndDeploy(0, _bins, _machines);
//    }
//
//    //*************************************************************************
//    //仅用于 DataSetA 的代码
//    private static void ToSubmitDataSetA() {
//      var bins600 = GetBinsDataSetA(600);
//      var bins1024 = GetBinsDataSetA(1024);
//
//      //预先保存这些机器，否则下面分配了空机器后就不好区分了
//      var initOccupied600 = OccupiedMachines(600);
//      var initOccupied1024 = OccupiedMachines(1024);
//
//      //先把空的机器都占了
//      var idx600 = DeployOnIdle(bins600, IdleMachines(600)); // 1724 台
//      var idx1024 = DeployOnIdle(bins1024, IdleMachines(1024)); // 1761 台
//
//      //Console.WriteLine($"i600={idx600},i1024={idx1024}");
//
//      //初始occupied的机器中空出来了一些实例
//      idx600 = MigrateAndDeploy(idx600, bins600, initOccupied600);
//      idx1024 = MigrateAndDeploy(idx1024, bins1024, initOccupied1024);
//
//      Console.WriteLine($"i600={idx600},i1024={idx1024}");
//      Console.WriteLine(DeployedBinsCount);
//
//      //期望的最终状态：600的机器共占用2506台，1024的机器共占用3000台
//      //Console.WriteLine($"Occupied600={OccupiedMachines(600).Count}," +
//      //                  $"Occupied1024={OccupiedMachines(1024).Count}");
//
//      //foreach (var m in Machines) Console.WriteLine(m);
//    }
//
//    private static List<List<Instance>> GetBinsDataSetA(int capDisk) {
//      var bins = new List<List<Instance>>(3000);
//      var start = 0;
//      var end = 3000;
//      if (capDisk == 600) {
//        //search文件中3000行之后的方案是600 GB硬盘的机器的
//        start = 3000;
//        end = _bins.Count;
//      }
//
//      for (var i = start; i < end; i++) bins.Add(_bins[i]);
//
//      return bins;
//    }
//    //*/
//
//    //*************************************************************************
//    private static void ParseSearchResult(string searchFile) {
//      //格式
//      //total(0.500000,600): {80,100,80,100,80,80,80} (inst_6297,inst_20827,...)
//      var f = File.OpenText(searchFile);
//      string line;
//      while (null != (line = f.ReadLine())) {
//        // ReSharper disable once StringIndexOfIsCultureSpecific.1
//        var s = line.IndexOf("inst_");
//        if (s < 0) continue;
//
//        _bins.Add(line.Substring(s, line.Length - s - 1).CsvToInstList(_solution));
//      }
//
//      f.Close();
//    }
//
//    //对（初始状态）占用的机器按大小类型分类，并按cpu.avg排序
//    private static List<Machine> OccupiedMachines(int capDisk) {
//      return (from m in _machines
//        where m.InstSet.Count > 0 && m.CapDisk == capDisk
//        orderby m.InstSet.Sum(i => i.R.Cpu.Avg) //这里顺序很重要！
//        select m).ToList();
//    }
//
//    private static List<Machine> IdleMachines(int capDisk) {
//      return _machines.Where(m => m.IsIdle && m.CapDisk == capDisk).ToList();
//    }
//
//    private static int DeployOnIdle(List<List<Instance>> bins, IList<Machine> idleList) {
//      var i = 0;
//      while (i < idleList.Count && i < bins.Count) {
//        DeployBin(idleList[i], bins[i]);
//        i++;
//      }
//
//      return i;
//    }
//
//    private static int MigrateAndDeploy(int binIdx, List<List<Instance>> bins, IList<Machine> mList) {
//      var mIdx = 0;
//      while (binIdx < bins.Count && mIdx < mList.Count) {
//        var bin = bins[binIdx++];
//        var m = mList[mIdx++];
//
//        if (!DrainMachine(m, bin)) break;
//        DeployBin(m, bin);
//      }
//
//      return binIdx;
//    }
//
//    //将某机器上原有的实例清空，迁移到其它若干机器上去
//    private static bool DrainMachine(Machine m, List<Instance> bin) {
//      var instList = m.InstSet.ToList();
//      var instCnt = instList.Count;
//      var migratedInstCnt = 0;
//      //类似Firstfit Desc,将原有实例迁移到若干台机器上去
//      for (var i = instCnt - 1; i >= 0; i--) {
//        var inst = instList[i];
//        //实例本来就在方案中，不需要迁移，概率很小
//        //若跳过此检查，不影响最终成本分数，但会多迁移几次
//        if (bin.Contains(inst)) {
//          migratedInstCnt++; //计数还是要增加的
//          continue;
//        }
//
//        foreach (var n in _machines) {
//          if (m == n) continue;
//
//          if (n.TryPutInst(inst, _w)) {
//            migratedInstCnt++;
//            break; //FirstFit
//          }
//        }
//      }
//
//      if (migratedInstCnt != instCnt) {
//        Console.WriteLine($"Can not migrate existing instances at m_{m.Id}!");
//        return false;
//      }
//
//      return true;
//    }
//
//    private static void DeployBin(Machine m, List<Instance> bin) {
//      foreach (var inst in bin)
//        if (!m.TryPutInst(inst, _w))
//          throw new Exception($"Unkown Error, Deployed Failed!{m}");
//    }
//
//    //列表中所有实例是否都部署到了同一台机器上，而且都没有违反亲和性约束    
//    private static bool IsDeployed(List<Instance> bin) {
//      var m = bin[0].Machine;
//      if (m == null) return false;
//
//      var sameMachine = true;
//      foreach (var inst in bin)
//        if (inst.Machine != m || !inst.Deployed) {
//          sameMachine = false;
//          break;
//        }
//
//      return sameMachine;
//    }
//  }
//}

