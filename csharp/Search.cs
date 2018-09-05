using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using TPL = System.Threading.Tasks;
using static System.Console;
using static Tianchi.Util;

namespace Tianchi {
  public class Move {
    public readonly AppInst Inst;
    public readonly Machine MachineDest;

    public Move(Machine machineDest, AppInst inst) {
      MachineDest = machineDest;
      Inst = inst;
    }
  }

  public class NeighborByMove {
    public readonly Move Move;
    public double Delta;

    public NeighborByMove(Move move) {
      Move = move;
    }
  }

  public class SubmitEntry {
    public readonly int InstId;
    public readonly int MachineIdDest;
    public readonly int Round;

    public SubmitEntry(int round, int instId, int machineIdDest) {
      Round = round;
      InstId = instId;
      MachineIdDest = machineIdDest;
    }
  }

  public class Search {
    public const int NeighborSize = 10;
    public const int TabuLifespan = 4;
    public readonly object BestScoreLock = new object();
    public readonly Solution Solution;
    public readonly List<SubmitEntry> SubmitResult = new List<SubmitEntry>(capacity: 20000);
    public readonly Dictionary<Move, int> TabuKv = new Dictionary<Move, int>(capacity: 1000);
    private Solution _holdSol;
    private volatile bool _stop;

    public Search(Solution solution) {
      Solution = solution;
    }

    public double BestScore { get; private set; }

    public int Round { get; private set; }


    // 可以直接从 solution 搜索，
    // 也可以从磁盘保存的搜索结果继续搜索
    public string Run(int round, string submitIn = "", long timeout = 1 * Hour, int taskCnt = 20) {
      Round = round;
      if (!string.IsNullOrEmpty(submitIn)) {
        if (!File.Exists(submitIn)) {
          Error.WriteLine($"Error: Cannot find search file {submitIn}");
          Environment.Exit(exitCode: -1);
        } else {
          //用文件中的结果覆盖 Solution
          Solution.SetInitDeploy();
          SubmitResult.Clear();
          var r = Solution.ReadAppSubmit(Solution, submitIn, SubmitResult);
          //新开始一轮，就清空上一轮未决的实例，但最后一轮不清空
          if (Round > r && Round < 3) {
            Solution.ReleaseAllPendingInsts(Solution);
            Round = r + 1;
          }
        }
      }

      //注意顺序，_holdSol仍可能保存最后一轮未释放的机器状态
      _holdSol = Solution.Clone();
      Solution.ReleaseAllPendingInsts(Solution);

      BestScore = Solution.CalcActualScore();
      WriteLine($"[Run {Solution.DataSet.Id}@r{Round}] Init Score: {BestScore:0.00000}");

      _stop = false;

      CancelKeyPress += (sender, e) => {
        _stop = true;
        e.Cancel = true; //等待搜索任务自己返回
        WriteLine($"{e.SpecialKey} received");
      };

      var timer = new Timer(obj => {
          _stop = true;
          WriteLine($"Executing timeout: {timeout * 1.0 / Min:0.0} min");
        },
        state: null, dueTime: timeout, period: Timeout.Infinite);

      var tasks = new List<TPL.Task>(taskCnt);
      for (var i = 0; i < taskCnt; i++) {
        var t = TPL.Task.Run(() => TabuSearch());
        tasks.Add(t);
      }

      TPL.Task.WaitAll(tasks.ToArray());

      timer.Dispose();

      var f = "submit_app" +
              $"_{Solution.DataSet.Id}" +
              $"_{Round}" +
              $"_{Solution.ActualScore:0}.csv";

      Solution.SaveAppSubmit(SubmitResult, f);
      return f;
    }

    public void Stop() {
      _stop = true;
    }

    private void TabuSearch() {
      var tId = 999;
      if (TPL.Task.CurrentId.HasValue) {
        tId = TPL.Task.CurrentId.Value;
      }

      tId += 10; //保持两位数
      var failedCnt = 0;

      var moves = new List<Move>(NeighborSize);

      var rnd = new Random(tId);

      var cpu1 = new Series(Resource.T98); // 减少gc
      var cpu2 = new Series(Resource.T98);

      while (true) {
        if (_stop) {
          //WriteLine($"[Search t{tId}] Stopping ...");
          break;
        }

        var best = Sample(moves, rnd, cpu1, cpu2);
        if (best == null) {
          failedCnt += 1;
          if (failedCnt == 20) {
            break;
          }

          continue;
        }

        //实施迁移
        var inst = best.Move.Inst;
        var mSrc = best.Move.Inst.Machine;
        var mDest = best.Move.MachineDest;

        var uInst = _holdSol.AppInstKv[inst.Id];
        var uDest = _holdSol.MachineKv[mDest.Id];

        if (uInst.IsPending) { //?
          continue;
        }

        var m1 = mSrc;
        var m2 = mDest;
        //总是先锁定 Id 较小的机器，避免死锁
        if (m1.Id > m2.Id) {
          m1 = mDest;
          m2 = mSrc;
        }

        lock (m1) {
          lock (m2) {
            //并发时，机器状态在抽样后可能发生改变了，需要再检查一下
            var canMove = EvaluateMove(mDest, inst, cpu1, cpu2, out _);
            var uCanMove = EvaluateMove(uDest, uInst, cpu1, cpu2, out _);
            if (!canMove || !uCanMove) {
              continue;
            }

            mDest.TryPut(inst, ignoreCheck: true); //迁移后，从原机器移除
            uDest.TryPut(uInst, ignoreCheck: true, autoRemove: false); //脚踩两只船

            lock (Solution) {
              Solution.CalcActualScore();
              lock (BestScoreLock) {
                if (Solution.ActualScore >= BestScore) {
                  mSrc.TryPut(inst, ignoreCheck: true); //撤销
                  uDest.Remove(uInst);
                  uInst.Machine = uInst.PrevMachine;
                  uInst.IsDeployed = true;
                  uInst.PrevMachine = null;
                  continue;
                }

                BestScore = Solution.ActualScore;
              }
            }

            lock (SubmitResult) {
              SubmitResult.Add(new SubmitEntry(Round, inst.Id, mDest.Id));
            }

            lock (TabuKv) {
              UpdateTabuLifespan();
              TabuKv[best.Move] = TabuLifespan;
            }
          }
        }
      }
    }

    //与Solution同步修改 inst 部署，但不从旧机器移除 inst
    private static bool EvaluateMove(Machine mDest, AppInst inst, Series cpu1,
      Series cpu2,
      out double delta) {
      delta = double.MaxValue;

      var mSrc = inst.Machine;

      if (inst.IsPending || mSrc == mDest || !mDest.CanPut(inst)) {
        return false;
      }

      var scoreBefore = mSrc.Score + mDest.Score;

      mSrc.Usage.Cpu.ShrinkTo(cpu1); // 压缩到98维
      cpu1.Subtract(inst.R.Cpu);

      mDest.Usage.Cpu.ShrinkTo(cpu2);
      cpu2.Add(inst.R.Cpu);

      //如果 machine 有 pending 的inst，这里的计算没有扣除其资源和计数
      var scoreAfter = cpu1.Score(mSrc.CapCpu, mSrc.AppInstCount - 1) +
                       cpu2.Score(mSrc.CapCpu, mDest.AppInstCount + 1);

      delta = scoreAfter - scoreBefore;

      return delta < 0.0 && -delta > 0.0001;
    }

    //返回 Best Neighbor 或者 null
    private NeighborByMove Sample(List<Move> moves, Random rnd, Series cpu1, Series cpu2) {
      var neighborCnt = 0;
      var failedCnt = 0;

      moves.Clear();

      NeighborByMove best = null;

      for (var i = 0; i < NeighborSize; i++) {
        //采样机制：
        var mIdxSrc = rnd.Next(Solution.MachineCount);
        var mIdxDest = MachineIndexDest(rnd);
        if (mIdxSrc == mIdxDest) {
          i--;
          continue;
        }

        var found = false;
        var mSrc = Solution.Machines[mIdxSrc];
        var mDest = Solution.Machines[mIdxDest];

        var m1 = mSrc;
        var m2 = mDest;
        //总是先锁定 Id 较小的机器，避免死锁
        if (m1.Id > m2.Id) {
          m1 = mDest;
          m2 = mSrc;
        }

        if (!Monitor.TryEnter(m1)) {
          continue;
        }

        try {
          lock (m2) {
            //优先处理大型实例
            var list = mSrc.AppKv.Values.OrderByDescending(inst => inst.R.Cpu.Avg);
            foreach (var inst in list) {
              if (Exists(mDest, inst, moves)) {
                continue; //换一个inst
              }

              var canMove = EvaluateMove(mDest, inst, cpu1, cpu2, out var delta);

              var uInst = _holdSol.AppInstKv[inst.Id];
              var uDest = _holdSol.MachineKv[mDest.Id];
              // 使用 _holdSol 的检查结果要比实际更严格一点
              var uCanMove = EvaluateMove(uDest, uInst, cpu1, cpu2, out _);

              if (!canMove || !uCanMove) {
                continue;
              }

              double score;
              lock (Solution) {
                score = Solution.CalcActualScore() + delta;
              }

              //禁忌和特赦
              lock (TabuKv) {
                if (Exists(mDest, inst, TabuKv.Keys.ToList()) && score >= BestScore) {
                  continue; //换一个inst
                }
              }

              var move = new Move(mDest, inst);
              var n = new NeighborByMove(move) {Delta = delta};
              if (best == null) {
                best = n;
              }

              if (n.Delta < best.Delta) {
                best = n;
              }

              neighborCnt++;
              moves.Add(move);

              found = true;
              break; //换一台机器
            }
          }
        } finally {
          Monitor.Exit(m1);
        }

        if (!found) {
          failedCnt++;
          i--;
        }

        if (failedCnt == 1000) {
          //WriteLine("[GetInitNeighbor] Too many failures during sampling neighbors");
          break;
        }
      }

      // 找到的 neighbor 可能少于 NeighborSize，
      // 但如果至少找到 4 个，就返回其中的最佳值
      return neighborCnt >= 4 ? best : null;
    }

    private static bool Exists(Machine mDest, AppInst inst, IList<Move> list) {
      for (var i = 0; i < list.Count; i++) {
        var move = list[i];
        if (move.MachineDest == mDest && move.Inst == inst) {
          return true;
        }
      }

      return false;
    }

    private void UpdateTabuLifespan() {
      var list = TabuKv.Keys.ToList();
      foreach (var tabu in list) {
        TabuKv[tabu]--;
        if (TabuKv[tabu] == 0) {
          TabuKv.Remove(tabu);
        }
      }
    }

    private int MachineIndexDest(Random rnd) {
      int idx;

      var rate = rnd.Next(maxValue: 100);

      var dsId = Solution.DataSet.Id;
      // c 和 d 均有 9000 台机器，前 6000 台是小型机器
      if (dsId == DataSetId.C || dsId == DataSetId.D) {
        if (rate > 30) { // todo: tuning 选择大型机器的概率
          idx = rnd.Next(maxValue: 3000) + 6000; //大型机器的范围
        } else {
          idx = rnd.Next(maxValue: 6000);
        }
      } else if (dsId == DataSetId.E) {
        // e 有 8000 台机器，前 6000 台是小型机器
        if (rate > 50) {
          idx = rnd.Next(maxValue: 2000) + 6000;
        } else {
          idx = rnd.Next(maxValue: 6000);
        }
      } else {
        // a 和 b 均有 8000 台大型机器
        // a, b的分数都在5000以下，故目标机器的范围不需要太大
        idx = rnd.Next(maxValue: 5000);
      }

      return idx;
    }
  }
}
