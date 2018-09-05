using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TPL = System.Threading.Tasks;
using static Tianchi.Util;

namespace Tianchi {
  public partial class AppSearch {
    public const int NeighborSize = 10;
    public const int TabuLifespan = 4;
    public readonly Dictionary<AppMove, int> TabuKv = new Dictionary<AppMove, int>(capacity: 1000);

    public string RunTabuSearch(int round, string submitIn = "", long timeout = 1 * Hour,
      int taskCnt = 20) {
      return Run(TabuSearch, round, submitIn, timeout, taskCnt);
    }

    private void TabuSearch() {
      var tId = 999;
      if (TPL.Task.CurrentId.HasValue) {
        tId = TPL.Task.CurrentId.Value;
      }

      tId += 10; //保持两位数
      var failedCnt = 0;

      var moves = new List<AppMove>(NeighborSize);

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
        var inst = best.AppMove.Inst;
        var mSrc = best.AppMove.Inst.Machine;
        var mDest = best.AppMove.MachineDest;

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
              TabuKv[best.AppMove] = TabuLifespan;
            }
          }
        }
      }
    }

    //返回 Best Neighbor 或者 null
    private NeighborAppMove Sample(List<AppMove> moves, Random rnd, Series cpu1, Series cpu2) {
      var neighborCnt = 0;
      var failedCnt = 0;

      moves.Clear();

      NeighborAppMove best = null;

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
              var uInst = _holdSol.AppInstKv[inst.Id];
              var uDest = _holdSol.MachineKv[mDest.Id];

              if (uInst.IsPending) {
                continue;
              }

              if (Exists(mDest, inst, moves)) {
                continue; //换一个inst
              }

              var canMove = EvaluateMove(mDest, inst, cpu1, cpu2, out var delta);
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

              var move = new AppMove(mDest, inst);
              var n = new NeighborAppMove(move) {Delta = delta};
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

    private static bool Exists(Machine mDest, AppInst inst, IList<AppMove> list) {
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
        idx = rnd.Next(maxValue: 8000);
      }

      return idx;
    }
  }
}
