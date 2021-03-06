using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using TPL = System.Threading.Tasks;
using static System.Console;
using static Tianchi.Util;

namespace Tianchi {
  //TODO: Unnaive Search!
  public class Search {
    private double _searchScore; // 共享变量，dotnet没有volatile double

    private volatile bool _stop;

    // 可以直接从 solution 搜索，
    // 也可以从磁盘保存的搜索结果继续搜索，默认超时时间是 5 h
    public void Run(Solution solution, string searchFile = "",
      long timeout = 5 * Hour, int taskCnt = 14) {
      if (!string.IsNullOrEmpty(searchFile)) {
        if (!File.Exists(searchFile)) {
          Error.WriteLine($"Error: Cannot find search file {searchFile}");
          Environment.Exit(exitCode: -1);
        } else {
          ReadResult(solution, searchFile); //用保存的结果覆盖 solution
        }
      }

      _searchScore = solution.ActualScore;
      _stop = false;

      CancelKeyPress += (sender, e) => {
        _stop = true;
        e.Cancel = true; //等待搜索任务自己返回
        WriteLine($"{e.SpecialKey} received");
      };

      var timer = new Timer(obj => {
          _stop = true;
          WriteLine("Executing timeout");
        },
        state: null, dueTime: timeout, period: Timeout.Infinite); // 24 hours

      var tasks = new List<TPL.Task>(taskCnt);
      for (var i = 0; i < taskCnt; i++) {
        var t = TPL.Task.Run(() => {
          while (TryRandomSearch(solution)) {
          }
        });
        tasks.Add(t);
      }

      TPL.Task.WaitAll(tasks.ToArray());

      timer.Dispose();

      WriteLine($"== {solution.ActualScore:0.000000} vs {_searchScore:0.000000}");
      SaveResult(solution);
    }

    public void Stop() {
      _stop = true;
    }

    private bool TryRandomSearch(Solution solution) {
      var shuffledIndexes = solution.MachineCount.ToRangeArray();
      var machines = solution.Machines;
      shuffledIndexes.Shuffle();
      var tId = TPL.Task.CurrentId;
      var hasChange = false;
      var u1 = new Resource(isT1470: false); //TODO: T1470 vs T98
      var u2 = new Resource(isT1470: false);
      var diff = new Resource(isT1470: false);
      var shrink = new Resource(isT1470: false);

      foreach (var i in shuffledIndexes) {
        foreach (var j in shuffledIndexes) {
          if (_stop) {
            WriteLine($"[Search@t{tId}] Stopping ...");
            return false;
          }

          if (i == j) {
            continue;
          }

          var m1 = machines[i];
          var m2 = machines[j];

          if (m1.Id > m2.Id) {
            m1 = machines[j];
            m2 = machines[i];
          }

          // move inst 
          if (Monitor.TryEnter(m1)) {
            try {
              lock (m2) {
                foreach (var inst2 in m2.AppInstSet.ToList()) { //ToList()即取快照

                  //TODO: 这里是将 m2 的 move 到 m1，可以遍历m2的所有实例
                  if (!TryMove(m1, inst2, out var deltaMove)) {
                    continue;
                  }

                  //delta == 0.0 表示两个机器 cpu util 移动前后都没有超过 0.5
                  UpdateScore(deltaMove);
                  WriteLine($"{_searchScore:0.000000}, [{tId}], " +
                            $"move inst_{inst2.Id} @ m_{m2.Id} -> m_{m1.Id}");
                  hasChange = true;
                }
              }
            } finally {
              Monitor.Exit(m1);
            }
          }

          // swap by app  
          if (Monitor.TryEnter(m1)) {
            try {
              foreach (var inst1 in m1.AppKv.Values.ToList()) {
                lock (m2) {
                  foreach (var inst2 in m2.AppKv.Values.ToList()) {
                    if (inst1.App == inst2.App) {
                      continue;
                    }

                    if (m2 != inst2.Machine) {
                      continue; //inst2刚在本线程的上轮循环swap了
                    }

                    if (!TrySwap(inst1, inst2, u1, u2, diff, shrink, out var deltaSwap)) {
                      continue;
                    }

                    UpdateScore(deltaSwap);
                    //WriteLine($"{_searchScore:0.000000}, [{tId}], " +
                    //          $"swap inst_{inst1.Id} <-> inst_{inst2.Id}");
                    hasChange = true;
                    break; //inst1已经swap了，外层循环继续检查下一个实例
                  }
                }
              }
            } finally {
              Monitor.Exit(m1);
            }
          }
        }

        WriteLine($"[Search@t{tId}]: {_searchScore:0.000000} @ m_{machines[i].Id}");
      }

      return hasChange;
    }

    //将inst移动到mDest，如果移动失败，deta为正数，或double.MaxValue
    //TODO: Move到空机器
    private static bool TryMove(Machine mDest, AppInst inst, out double delta) {
      delta = double.MaxValue;

      if (!mDest.HasApp || !mDest.CanPut(inst)) {
        return false;
      }

      var mSrc = inst.Machine;
      var scoreBefore = mSrc.Score + mDest.Score;
      mDest.TryPut(inst, ignoreCheck: true);
      var scoreAfter = mSrc.Score + mDest.Score;

      delta = scoreAfter - scoreBefore;

      if (delta > 0.0
          // delta == 0.0 && scoreAfter == 2.0
          // 即 move 前后两个机器的 cpu util 均小于 0.5，需要 move
          // ReSharper disable once CompareOfFloatsByEqualityOperator
          || delta == 0.0 && scoreAfter > 2.0
          || delta < 0.0 && delta > -0.00001) {
        mSrc.TryPut(inst, ignoreCheck: true); //恢复原状
      }

      return delta <= 0.0;
    }

    //如果交换失败，deta为正数，或double.MaxValue
    //引入u1,u2,diff,shrink这几个参数是为了减少GC
    private static bool TrySwap(AppInst inst1, AppInst inst2,
      Resource u1, Resource u2, Resource diff, Resource shrink, out double delta) {
      var m1 = inst1.Machine;
      var m2 = inst2.Machine;

      diff.DiffOf(inst1.R, inst2.R);

      delta = double.MaxValue;
      m1.Usage.ShrinkTo(shrink);
      u1.DiffOf(shrink, diff);
      if (u1.AnyLargerThan(m1.Capacity)) {
        return false;
      }

      m2.Usage.ShrinkTo(shrink);
      u2.SumOf(shrink, diff);
      if (u2.AnyLargerThan(m2.Capacity)) {
        return false;
      }

      m1.Usage.Cpu.ShrinkTo(shrink.Cpu);
      var scoreBefore = shrink.Cpu.Score(m1.CapCpu, m1.AppInstCount);
      m2.Usage.Cpu.ShrinkTo(shrink.Cpu);
      scoreBefore += shrink.Cpu.Score(m2.CapCpu, m2.AppInstCount);

      delta = u1.Cpu.Score(m1.CapCpu, m1.AppInstCount) + u2.Cpu.Score(m2.CapCpu, m2.AppInstCount)
              - scoreBefore;

      //期望delta是负数，且绝对值越大越好
      if (delta >= 0.0 || delta < 0.0 && delta > -0.00001) {
        return false;
      }

      if (HasConflict(inst1, inst2) ||
          HasConflict(inst2, inst1)) {
        return false;
      }

      m1 = inst1.Machine;
      m2 = inst2.Machine;

      m1.TryPut(inst2, ignoreCheck: true);
      m2.TryPut(inst1, ignoreCheck: true);

      return true;
    }

    // 假设从机器上移除 instOld 之后, 检查 instNew 是否有亲和冲突
    // 注意参数顺序
    private static bool HasConflict(AppInst instOld, AppInst instNew) {
      var m = instOld.Machine;
      var appCountKv = m.AppCountKv; //直接修改

      var appOld = instOld.App;

      //appOld的所有实例都在之前的循环move到别的机器了
      if (!appCountKv.ContainsKey(appOld)) {
        return false;
      }

      var appOldCnt = appCountKv[appOld];
      if (appOldCnt == 1) {
        appCountKv.Remove(appOld);
      } else {
        appCountKv[appOld] = appOldCnt - 1;
      }

      var result = m.IsConflictWith(instNew);
      appCountKv[appOld] = appOldCnt; //恢复原状

      return result;
    }

    //CAS原子操作
    private void UpdateScore(double delta) {
      double init, update;
      do {
        init = _searchScore;
        update = init + delta;
        // ReSharper disable once CompareOfFloatsByEqualityOperator
      } while (init != Interlocked.CompareExchange(ref _searchScore, update,
                 init));
    }

    public static string SaveResult(Solution solution) {
      if (!Solution.CheckAppInterference(solution) || !Solution.CheckResource(solution)) {
        WriteLine("[SaveResult] Bad Result!");
        return string.Empty;
      }

      var machines = solution.Machines;

      //保存到项目的search-result/目录下
      var outputPath = "search-result/" +
                       $"search_{solution.DataSet.Id}" +
                       $"_{solution.ActualScore:0.00}".Replace(oldChar: '.', newChar: '_') +
                       $"_{solution.MachineCountHasApp}m";

      WriteLine($"Writing to {outputPath}");
      var w = File.CreateText(outputPath);

      //保持确定的机器的顺序，而且保留空闲的机器
      //使 search result 的行号与机器Id对应
      foreach (var m in machines) {
        w.WriteLine(m.ToSearchStr());
      }

      w.Close();
      return outputPath;
    }

    public static void ReadResult(Solution solution, string searchFile) {
      solution.ClearAppDeploy(); //重置状态
      var machines = solution.Machines;

      //格式：
      //total(0.500000,600): {80,100,80,100,80,80,80} (inst_6297,inst_20827,...)
      var f = File.OpenText(searchFile);
      string line;
      var i = 0;
      while (null != (line = f.ReadLine())) {
        var m = machines[i++]; //TODO: 行号与机器Id严格对应

        // ReSharper disable once StringIndexOfIsCultureSpecific.1
        var s = line.IndexOf("inst_");
        if (s < 0) {
          continue; //跳过空闲机器
        }

        var instList = line.Substring(s, line.Length - s - 1)
          .CsvToAppInstList(solution);
        foreach (var inst in instList) {
          m.TryPut(inst, ignoreCheck: true); //TODO: ignoreCheck?
        }
      }

      f.Close();
    }
  }
}
