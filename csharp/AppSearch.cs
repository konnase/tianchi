using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using TPL = System.Threading.Tasks;
using static System.Console;
using static Tianchi.Util;

namespace Tianchi {
  public class AppMove {
    public readonly AppInst Inst;
    public readonly Machine MachineDest;

    public AppMove(Machine machineDest, AppInst inst) {
      MachineDest = machineDest;
      Inst = inst;
    }
  }

  public class NeighborAppMove {
    public readonly AppMove AppMove;
    public double Delta;

    public NeighborAppMove(AppMove move) {
      AppMove = move;
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

  public partial class AppSearch {
    public readonly object BestScoreLock = new object();
    public readonly Solution Solution;
    public readonly List<SubmitEntry> SubmitResult = new List<SubmitEntry>(capacity: 20000);
    private Solution _holdSol;
    private volatile bool _stop;

    public AppSearch(Solution solution) {
      Solution = solution;
    }

    public double BestScore { get; private set; }

    public int Round { get; private set; }

    // 可以直接从 solution 搜索，
    // 也可以从磁盘保存的搜索结果继续搜索
    private string Run(Action search, int round, string submitIn, long timeout, int taskCnt) {
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
        var t = TPL.Task.Run(search);
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
  }
}
