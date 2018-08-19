using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Tianchi {
  public static class NaiveSearch {
    private static double _searchTotalScore; //共享的累积量，dotnet不支持volatile double...
    private static Solution _solution;
    private static Machine[] _machines;

    private static volatile bool _stop;

    //可以直接从 solution 搜索，
    //也可以从磁盘保存的搜索结果继续搜索
    public static void Run(Solution solution, string searchFile = "", int taskCnt = 14) {
      _solution = solution;
      _machines = solution.Machines;

      if (!string.IsNullOrEmpty(searchFile)) {
        if (!File.Exists(searchFile)) {
          Console.Error.WriteLine($"Error: Cannot find search file {searchFile}");
          Environment.Exit(-1);
        } else {
          ParseSearchResult(searchFile); //用保存的结果覆盖 solution
        }
      }

      _searchTotalScore = _solution.TotalScore;

      Console.CancelKeyPress += (sender, e) => {
        _stop = true;
        e.Cancel = true; //等待搜索任务自己返回
        Console.WriteLine($"{e.SpecialKey} received");
      };

      var timer = new Timer(obj => {
          _stop = true;
          Console.WriteLine($"Executing timeout");
        },
        null, 24 * 60 * 60 * 1000, Timeout.Infinite); // 24 hours

      var tasks = new List<Task>(taskCnt);
      for (var i = 0; i < taskCnt; i++) {
        var t = Task.Run(() => {
          while (TrySearch()) {
          }
        });
        tasks.Add(t);
      }

      foreach (var t in tasks) t.Wait();

      timer.Dispose();

      Console.WriteLine($"{_solution.TotalScore:0.000000} vs {_searchTotalScore:0.000000}");
      SaveSearchResult();
    }

    private static bool TrySearch() {
      var shuffledIndexes = _solution.MachineCount.ToRangeArray();
      shuffledIndexes.Shuffle();
      var tId = Task.CurrentId;
      var hasChange = false;
      var u1 = new Resource();
      var u2 = new Resource();
      var diff = new Resource();

      foreach (var i in shuffledIndexes)
      foreach (var j in shuffledIndexes) {
        if (_stop) {
          Console.WriteLine($"[{tId}] Stopping ...");
          return false;
        }

        if (i == j) continue;

        var m1 = _machines[i];
        var m2 = _machines[j];

        Debug.Assert(m1.Id != m2.Id);

        if (m1.Id > m2.Id) {
          m1 = _machines[j];
          m2 = _machines[i];
        }

        // move inst 
        if (Monitor.TryEnter(m1))
          try {
            lock (m2) {
              foreach (var inst2 in m2.InstSet.ToList()) { //ToList()即取快照
                var deltaMove = TryMove(m1, inst2);
                if (deltaMove > 0.0) continue;

                //delta == 0.0 表示两个机器 cpu util 移动前后都没有超过 0.5
                UpdateScore(deltaMove);
                Console.WriteLine($"{_searchTotalScore:0.000000}, [{tId}], " +
                                  $"move inst_{inst2.Id} @ m_{m2.Id} -> m_{m1.Id}");
                hasChange = true;
              }
            }
          } finally {
            Monitor.Exit(m1);
          }

        // swap by app  
        if (Monitor.TryEnter(m1))
          try {
            foreach (var inst1 in m1.AppInstKv.Values.ToList())
              lock (m2) {
                foreach (var inst2 in m2.AppInstKv.Values.ToList()) {
                  if (inst1.App == inst2.App) continue;
                  if (m2 != inst2.Machine) continue; //inst2刚在本线程的上轮循环swap了

                  var deltaSwap = TrySwap(inst1, inst2, u1, u2, diff);
                  if (deltaSwap > 0.0) continue;

                  UpdateScore(deltaSwap);

                  Console.WriteLine($"{_searchTotalScore:0.000000}, [{tId}], " +
                                    $"swap inst_{inst1.Id} <-> inst_{inst2.Id}");
                  hasChange = true;
                  break; //inst1已经swap了，外层循环继续检查下一个实例
                  //*/
                }
              }
          } finally {
            Monitor.Exit(m1);
          }
      }

      return hasChange;
    }

    //将inst移动到mDest，如果移动成功，返回负的分数差值，否则返回double.MaxValue
    private static double TryMove(Machine mDest, Instance inst) {
      var delta = double.MaxValue;

      if (!mDest.IsIdle && mDest.CanPutInst(inst)) {
        var mSrc = inst.Machine;
        var scoreBefore = mSrc.Score + mDest.Score;
        mDest.TryPutInst(inst, ignoreCheck: true);
        var scoreAfter = mSrc.Score + mDest.Score;

        delta = scoreAfter - scoreBefore;

        //delta
        if (delta > 0.0
            // delta == 0.0 && scoreAfter == 2.0
            // 即 move 前后两个机器的 cpu util 均小于 0.5，需要 move
            // ReSharper disable once CompareOfFloatsByEqualityOperator
            || delta == 0.0 && scoreAfter > 2.0
            || delta < 0.0 && delta > -0.00001)
          mSrc.TryPutInst(inst, ignoreCheck: true); //恢复原状
      }

      return delta;
    }

    //如果交换成功，返回负的分数差值，否则返回double.MaxValue
    //引入u1,u2,diff这三个参数是为了减少GC
    private static double TrySwap(Instance inst1, Instance inst2,
      Resource u1, Resource u2, Resource diff) {
      var m1 = inst1.Machine;
      var m2 = inst2.Machine;

      diff.CopyFrom(inst1.R).Subtract(inst2.R);

      u1.CopyFrom(m1.Usage).Subtract(diff);
      if (u1.IsOverCap(m1.Capacity)) return double.MaxValue;

      u2.CopyFrom(m2.Usage).Add(diff);
      if (u2.IsOverCap(m2.Capacity)) return double.MaxValue;

      var scoreBefore = m1.Score + m2.Score;
      var delta = u1.Cpu.Score(m1.CapCpu) + u2.Cpu.Score(m2.CapCpu)
                  - scoreBefore;

      //期望delta是负数，且绝对值越大越好
      if (delta >= 0.0 || delta < 0.0 && delta > -0.00001) return double.MaxValue;

      if (HasConflict(inst1, inst2)) return double.MaxValue;
      if (HasConflict(inst2, inst1)) return double.MaxValue;

      m1 = inst1.Machine;
      m2 = inst2.Machine;

      m1.TryPutInst(inst2, ignoreCheck: true);
      m2.TryPutInst(inst1, ignoreCheck: true);

      return delta;
    }

    // 假设从机器上移除 instOld 之后, 检查 instNew 是否有亲和冲突
    // 注意参数顺序
    private static bool HasConflict(Instance instOld, Instance instNew) {
      var m = instOld.Machine;
      var appCountKv = m.AppCountKv; //直接修改

      var appOld = instOld.App;

      //appOld的所有实例都在之前的循环move到别的机器了
      if (!appCountKv.ContainsKey(appOld)) return false;

      var appOldCnt = appCountKv[appOld];
      if (appOldCnt == 1)
        appCountKv.Remove(appOld);
      else
        appCountKv[appOld] = appOldCnt - 1;

      var result = m.HasConflictWithInst(instNew);
      appCountKv[appOld] = appOldCnt; //恢复原状

      return result;
    }

    //CAS原子操作
    private static void UpdateScore(double delta) {
      double init, update;
      do {
        init = _searchTotalScore;
        update = init + delta;
        // ReSharper disable once CompareOfFloatsByEqualityOperator
      } while (init != Interlocked.CompareExchange(ref _searchTotalScore, update, init));
    }

    private static void SaveSearchResult() {
      if (!_solution.FinalCheck()) return;

      //保存到项目search-result/目录
      var output = $"search-result/" +
                   $"search_{_solution.DataSet.Id}" +
                   $"_{_solution.TotalScore:0.00}".Replace('.', '_') +
                   $"_{_solution.UsedMachineCount}m";

      Console.WriteLine($"Writing to {output}");
      var w = File.CreateText(output);

      //保持确定的机器的顺序，而且保留空闲的机器
      //使 search result 的行号与机器Id对应
      foreach (var m in _machines) w.WriteLine(m.ToSearchStr());

      w.Close();
    }

    private static void ParseSearchResult(string searchFile) {
      _solution.ClearDeploy(); //重置状态

      //格式：
      //total(0.500000,600): {80,100,80,100,80,80,80} (inst_6297,inst_20827,...)
      var f = File.OpenText(searchFile);
      string line;
      var i = 0;
      while (null != (line = f.ReadLine())) {
        var m = _machines[i++];

        // ReSharper disable once StringIndexOfIsCultureSpecific.1
        var s = line.IndexOf("inst_");
        if (s < 0) continue; //跳过空闲机器

        var instList = line.Substring(s, line.Length - s - 1).CsvToInstList(_solution);
        foreach (var inst in instList) m.TryPutInst(inst, ignoreCheck: true); //TODO: ignoreCheck?
      }

      f.Close();
    }

    //TODO: search result to submit.csv
  }
}