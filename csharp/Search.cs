using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Tianchi {
  public partial class Program {
    private static double _searchTotalScore; //共享的累积量，dotnet不支持volatile double...

    private static volatile bool _stop;

    private static void RunSearch(string[] args) {
      var searchFile = string.Empty;
      var taskCnt = 6;

      if (args.Length == 1) {
        searchFile = $"{_projectPath}/{args[0]}";
      } else if (args.Length == 2) {
        searchFile = $"{_projectPath}/{args[0]}";
        taskCnt = int.Parse(args[1]);
      } else {
        //需将 csharp\bin\Debug\netcoreapp2.1目录构建的.dll（及同名的runtimeconfig.json）手动拷贝到项目目录，
        //并修改文件名...
        Console.WriteLine("Usage:\n" +
                          "dotent search.dll <search file> <tasks=6>\n" +
                          "Example:\n" +
                          "dotnet search.dll search-result/search_6091_30_5024m 10\n" +
                          "dotnet search.dll search-result/search_6091_30_5024m");
        Environment.Exit(-1);
      }

      if (!File.Exists(searchFile)) {
        Console.Error.WriteLine($"Error: Cannot find search file {searchFile}");
        Environment.Exit(-1);
      }

      ReadAllData(DataSet);
      VerifySearchResult(searchFile);

      // ReSharper disable once InconsistentlySynchronizedField
      _searchTotalScore = TotalScore;

      Console.CancelKeyPress += (sender, e) => {
        _stop = true;
        e.Cancel = true; //等待搜索任务自己返回
        Console.WriteLine($"{e.SpecialKey} received");
      };

      var tasks = new List<Task>(taskCnt);
      for (var i = 0; i < taskCnt; i++) {
        var t = Task.Factory.StartNew(() => {
          while (TrySearch()) {
          }
        });
        tasks.Add(t);
      }

      foreach (var t in tasks) t.Wait();

      Console.WriteLine(TotalScore);
      SaveSearchResult();
    }

    private static bool TrySearch() {
      var shuffledIndexes = MachineCount.ToRangeArray();
      shuffledIndexes.Shuffle();
      var tId = Thread.CurrentThread.ManagedThreadId;
      var hasChange = false;
      var u1 = new Resource();
      var u2 = new Resource();
      var diff = new Resource();

      foreach (var i in shuffledIndexes)
      foreach (var j in shuffledIndexes) {
        if (_stop) {
          Console.WriteLine($"[{tId}] Stoping ...");
          return false;
        }

        if (i == j) continue;

        var m1 = Machines[i];
        var m2 = Machines[j];

        Debug.Assert(m1.Id != m2.Id);

        if (m1.Id > m2.Id) {
          m1 = Machines[j];
          m2 = Machines[i];
        }

        lock (m1) {
          lock (m2) {
            foreach (var inst1 in m1.AppInstKv.Values.ToList()) //ToList()取快照
            foreach (var inst2 in m2.AppInstKv.Values.ToList()) {
              if (inst1.App.Id == inst2.App.Id) continue;
              if (m1 != inst1.Machine || m2 != inst2.Machine) continue; //实例已经被别的线程处理过了

              var delta = TrySwap(inst1, inst2, u1, u2, diff);
              if (delta > 0.0) continue;

              UpdateScore(delta);

              //即便采用了原子操作，输出的值仍可能不是刚才计算出来的值，
              //但仍是某个任务计算出来的结果，也可以接受
              Console.WriteLine($"{_searchTotalScore:0.000000}, swap inst_{inst1.Id} <-> inst_{inst2.Id}");
              hasChange = true;
            }
          }
        }
      }

      return hasChange;
    }

    //如果交换成功，返回负的分数差值，否则返回double.MaxValue
    private static double TrySwap(Instance inst1, Instance inst2,
      Resource u1, Resource u2, Resource diff) { //引入u1,u2,diff这三个参数是为了减少GC
      var i1 = inst1;
      var i2 = inst2;
      Debug.Assert(inst1.Id != inst2.Id);
      if (i1.Id > i2.Id) {
        i1 = inst2;
        i2 = inst1;
      }

      lock (i1) {
        lock (i2) {
          var m1 = inst1.Machine;
          var m2 = inst2.Machine;

          diff.Copy(inst1.R).Subtract(inst2.R);

          u1.Copy(m1.Usage).Subtract(diff);
          if (u1.IsOverCap(m1.Capacity)) return double.MaxValue;

          u2.Copy(m2.Usage).Add(diff);
          if (u2.IsOverCap(m2.Capacity)) return double.MaxValue;

          var delta = u1.Cpu.Score(m1.CapCpu) + u2.Cpu.Score(m2.CapCpu)
                      - (m1.Score + m2.Score);

          //期望delta是负数，且绝对值越大越好
          if (delta > 0.0 || -delta < 0.00001) return double.MaxValue;

          if (HasConflict(inst1, inst2) || HasConflict(inst2, inst1)) return double.MaxValue;

          m1 = inst1.Machine;
          m2 = inst2.Machine;
          m1.RemoveInst(inst1);
          m2.RemoveInst(inst2);
          var ok1 = m1.TryPutInst(inst2);
          var ok2 = m2.TryPutInst(inst1);
          if (!ok1 || !ok2)
            throw new Exception($"swap error! ok1 {ok1}, ok2 {ok2}");

          return delta;
        }
      }
    }

    // 假设从机器上移除 instOld 之后, 检查 instNew 是否有亲和冲突
    // 注意参数顺序
    private static bool HasConflict(Instance instOld, Instance instNew) {
      var m = instOld.Machine;
      m.RemoveInst(instOld);

      var result = m.HasConflictWithInst(instNew);
      m.TryPutInst(instOld, ignoreCheck: true); // 恢复原状
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
      if (FinalCheck()) {
        var output = $"{_projectPath}/search-result/" +
                     $"search_{TotalScore:0.00}".Replace('.', '_') +
                     $"_{UsedMachineCount}m";
        Console.WriteLine($"Writing to {output}");
        var w = File.CreateText(output);

        var list = from m in Machines
          where !m.IsIdle
          orderby m.CapDisk descending, m.Score descending
          select m;
        //注意机器的顺序
        //排序后分数小（1.00）的机器在文件尾部，便于修正丢失的实例
        foreach (var m in list)
          w.WriteLine(m.ToSearchStr());

        w.Close();
      }
    }
  }
}