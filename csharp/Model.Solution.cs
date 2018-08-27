using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using static System.Console;

namespace Tianchi {
  public partial class Solution {
    //AppInstCsv里有实例Id和部署信息，分两次读取
    private readonly string _appInstCsv;

    public readonly Dictionary<int, AppInst> AppInstKv; //仅用于查询
    public readonly AppInst[] AppInsts; //在线实例是固定的
    public readonly DataSet DataSet;
    public readonly Dictionary<int, Machine> MachineKv; //仅用于查询
    public readonly Machine[] Machines; //在线实例的部署状态保存在机器对象中

    private Solution(DataSet ds, string appInstCsv) {
      DataSet = ds;
      Machines = new Machine[MachineCount];
      MachineKv = new Dictionary<int, Machine>(MachineCount);
      AppInsts = new AppInst[AppInstCount];
      AppInstKv = new Dictionary<int, AppInst>(AppInstCount);
      _appInstCsv = appInstCsv;
    }

    public int AppInstCount => DataSet.AppInstCount;
    public int MachineCount => DataSet.MachineCount;

    #region 从文件读机器和在线实例数据，克隆

    public static Solution Read(DataSet dataSet, string machineCsv, string appInstCsv,
      bool isAlpha10 = false, bool withInitDeploy = true) {
      var solution = new Solution(dataSet, appInstCsv);
      solution.ReadMachine(machineCsv, isAlpha10);
      solution.ReadAppInst(appInstCsv);
      solution.SetKv();
      if (withInitDeploy) {
        solution.ReadInitDeploy();
      }

      return solution;
    }

    // 使 this 与 src 的部署状态保持一致，既同步已部署的实例，也同步未部署的
    private void CopyAppDeploy(Solution src) {
      if (src.DataSet != DataSet) {
        throw new Exception($"[CopyAppDeploy]: DataSet {DataSet.Id} mismatch!");
      }

      src.AppInsts.ForEach(inst => {
        var cloneInst = AppInstKv[inst.Id];
        if (inst.Machine == null) {
          cloneInst.Machine?.Remove(cloneInst);
        } else {
          var cloneM = MachineKv[inst.Machine.Id]; //Id相同，但object不同
          cloneM.TryPut(cloneInst, ignoreCheck: true);
        }

        cloneInst.IsDeployed = inst.IsDeployed;
      });
    }

    private void SetInitAppDeploy() {
      CopyAppDeploy(DataSet.InitSolution);
    }

    public Solution Clone(bool withDeploy = true) {
      var clone = new Solution(DataSet, _appInstCsv);
      Machines.ForEach((m, i) => clone.Machines[i] = m.Clone());
      AppInsts.ForEach((inst, i) => clone.AppInsts[i] = inst.Clone());
      clone.SetKv();
      if (withDeploy) {
        clone.CopyAppDeploy(this);
      }

      //Job的部署状态保存在 BatchKv，创建Solution时就会创建空白的 BatchKv，
      //其中保存的 JobBatch 是不可变对象，不复制
      return clone;
    }

    #endregion

    #region  生成分轮次的 submit.csv

    //TODO: Test on Semi DataSets 
    // 注意：会修改 clone 方案中的对象！
    // 要求 final 中所有实例都已经部署了，但 clone（初始部署） 可以有未部署的实例
    // clone 和 final 两个方案中 Inst 和 Machine 的Id值相同，但 Object 不同
    // 写完之后不关闭文件！
    public static void SaveAppSubmit(Solution final, Solution clone,
      StreamWriter writer = null, int maxRound = 3) {
      var migrateKv = GetDiff(final, clone, false);
      var deployKv = GetDiff(final, clone, true);

      WriteLine($"[SaveSubmit]: {migrateKv.Count} to migrate; {deployKv.Count} to deploy");

      // TODO: 排序
      // 只对 migrateInstKv 排序 
      var sortedMigrate = (from kv in migrateKv
        orderby kv.Key.Machine.Id descending
        select kv).ToList();

      // 机器只排序一次，把大型的，空闲资源多的机器排到前面
      var sortedMachines = (from m in clone.Machines
        orderby m.CapDisk descending, m.Usage.Disk, m.Id
        select m).ToList();

      var pendingSet = new HashSet<AppInst>(1000);
      var failedKv = new Dictionary<AppInst, int>();

      var round = 1;

      // 迁移
      // 注意，round 是从1开始的，循环的轮次比 maxRound 少一轮
      while (round < maxRound && migrateKv.Count > 0) {
        Migrate(clone, migrateKv, failedKv, pendingSet, sortedMigrate,
          sortedMachines, writer, round);

        // 释放了原机器上的资源，一轮就结束了
        ReleasePendingSet(pendingSet);
        round++;
      }

      // 循环结束了还有一轮的机会
      if (migrateKv.Count > 0) {
        Migrate(clone, migrateKv, failedKv, pendingSet, sortedMigrate,
          sortedMachines, writer, round);
      }

      var migrateCnt = migrateKv.Count;
      if (migrateCnt != 0) {
        WriteLine($"[SaveSubmit]: Migrating Failed {migrateCnt}#");
      }

      // 部署，只在最后一轮进行
      // 正常的话，完成迁移后，部署不会出现失败的实例
      Deploy(clone, deployKv, failedKv, pendingSet, writer, round);

      var deployCnt = deployKv.Count;
      if (deployCnt != 0) {
        WriteLine($"[SaveSubmit]: Deploying Failed {deployCnt}#");
        var cnt = 0;
        foreach (var kv in deployKv) {
          cnt++;
          WriteLine($"[SaveSubmit]: {cnt} - {kv.Key}\t{clone.MachineKv[kv.Value]}");
          if (cnt == 5) {
            break;
          }
        }
      }

      if (migrateCnt + deployCnt == 0) {
        WriteLine("[SaveSubmit]: OK!");
      }
    }

    private static void Migrate(Solution clone, Dictionary<AppInst, int> migrateKv,
      Dictionary<AppInst, int> failedKv, HashSet<AppInst> pendingSet,
      List<KeyValuePair<AppInst, int>> sortedMigrate, List<Machine> machines,
      StreamWriter writer, int round) {
      var failedCnt = int.MaxValue;
      var preFailedCnt = int.MaxValue;

      // 循环，直到没有迁移失败的实例，或者无法再处理迁移失败的实例了
      while (failedCnt != 0) {
        Deploy(clone, migrateKv, failedKv, pendingSet, writer, round, sortedMigrate);

        failedCnt = failedKv.Count;

        if (preFailedCnt == int.MaxValue) {
          preFailedCnt = failedCnt;
        } else if (preFailedCnt != failedCnt) {
          preFailedCnt = failedCnt;
        } else {
          break;
        }

        // failedKv 中不同实例的源机器和目标机器存在循环，导致死锁
        // 需临时迁移到其它任意的机器
        if (failedCnt == 0) {
          continue;
        }

        WriteLine($"[SaveSubmit]: failedKv@{round} {failedKv.Count}#");
        var cnt = 0;

        // 取快照，因为循环内部会修改 failedKv
        var list = failedKv.ToList();
        foreach (var kv in list) {
          var inst = kv.Key;
          var mIdDest = kv.Value;
          foreach (var m in machines) {
            if (m.Id == mIdDest || inst.Machine == m) {
              continue;
            }

            // First Fit 到其它机器上
            if (m.TryPut(inst, autoRemove: false)) {
              pendingSet.Add(inst);
              writer?.WriteLine($"{round},inst_{inst.Id},m_{m.Id}");
              failedKv.Remove(inst);
              cnt++;
              break;
            }
          }
        }

        WriteLine($"[SaveSubmit]: failedKv@{round} {cnt}# migrated temporarily");
      }
    }

    private static void Deploy(Solution clone, Dictionary<AppInst, int> instKv,
      Dictionary<AppInst, int> failedKv, HashSet<AppInst> pendingSet,
      StreamWriter writer, int round, List<KeyValuePair<AppInst, int>> sortedList = null) {
      //
      failedKv.Clear();

      ICollection<KeyValuePair<AppInst, int>> list;
      if (sortedList != null) {
        list = sortedList;
      } else {
        list = instKv.ToList();
      }

      foreach (var kv in list) {
        var inst = kv.Key;
        if (!instKv.ContainsKey(inst)) {
          continue;
        }

        var mIdDest = kv.Value;
        var mDest = clone.MachineKv[mIdDest];
        if (mDest.TryPut(inst, autoRemove: false)) {
          pendingSet.Add(inst);
          instKv.Remove(inst);
          writer?.WriteLine($"{round},inst_{inst.Id},m_{mDest.Id}");
        } else {
          failedKv[inst] = mIdDest;
        }
      }
    }

    private static void ReleasePendingSet(HashSet<AppInst> pendingSet) {
      foreach (var inst in pendingSet) {
        inst.PreMachine?.Remove(inst, false);
      }

      pendingSet.Clear();
    }

    // 不会修改 init
    private static Dictionary<AppInst, int> GetDiff(Solution final, Solution init, bool isDeploy) {
      var diffKv = new Dictionary<AppInst, int>();
      foreach (var instZ in final.AppInsts) {
        var instA = init.AppInstKv[instZ.Id];
        if (isDeploy && instA.Machine == null) {
          var mIdDest = instZ.Machine.Id;
          diffKv[instA] = mIdDest;
        } else if (!isDeploy && instA.Machine != null) {
          var mIdSrc = instA.Machine.Id;
          var mIdDest = instZ.Machine.Id;
          if (mIdSrc != mIdDest) {
            diffKv[instA] = mIdDest;
          }
        }
      }

      return diffKv;
    }

    #endregion

    #region  Read Csv

    private void SetKv() {
      Machines.ForEach(m => MachineKv[m.Id] = m);
      AppInsts.ForEach(inst => AppInstKv[inst.Id] = inst);
    }

    // 读取机器，并按磁盘大小（降序）和Id（升序）排序
    private void ReadMachine(string csv, bool isAlpha10) {
      var list = new List<Machine>(10000);
      Util.ReadCsv(csv, line => {
        var m = Machine.Parse(line, isAlpha10);
        list.Add(m);
      });
      var sorted = from m in list
        orderby m.CapDisk descending, m.Id
        select m;
      var i = 0;
      foreach (var m in sorted) {
        Machines[i] = m;
        i++;
      }
    }

    // 读取实例，保持原有顺序！
    private void ReadAppInst(string csv) {
      var i = 0;
      Util.ReadCsv(csv, parts => {
          var instId = parts[0].Id();
          var appId = parts[1].Id();
          var app = DataSet.AppKv[appId];
          var inst = new AppInst(instId, app);
          app.InstCount++;
          AppInsts[i++] = inst;
        }
      );
    }

    // 读取数据集的初始部署
    // 注意：调用前需事先清空已有部署
    private void ReadInitDeploy() {
      Util.ReadCsv(_appInstCsv, parts => {
          var mId = parts[2].Id();

          // 可能初始状态没有分配机器
          if (mId == int.MinValue) {
            return;
          }

          var m = MachineKv[mId];
          var instId = parts[0].Id();
          var inst = AppInstKv[instId];

          // 官方的评测代码在初始化阶段忽略资源和亲和性检查，直接将 inst 添加到机器上。
          // 在评价阶段，如果提交的代码中有 inst 的新部署目标，才会将其迁移；
          // 在迁移之前向旧机器放置实例，就可能存在不必要的资源或亲和性冲突；
          // Fixed: 早先版本初赛数据集B的初始部署错误地使用了 cpuUtilLimit 检查，
          // 这里的限值应该是 1.0
          var canPut = m.CanPut(inst);
          m.TryPut(inst, ignoreCheck: true);

          // 因为 TryPutInst 函数会将 Deployed 置为true，
          // 而且添加inst后改变了机器状态，会增加资源使用和App个数，
          // 所以先保存判断结果，在机器上部署了实例后再修正 Deployed 标志
          // Fixed: 早先版本存在没有事先保存判断结果的Bug，反而获得了更好的分数
          // 可能是因为迁移了那些负载较重机器上的实例，这里不保持这个Bug
          inst.IsDeployed = canPut; //m.CanPutInst(inst); 
        }
      );
    }

    #endregion

    #region  Judge

    public int MachineCountHasApp => Machines.Sum(m => m.HasApp ? 1 : 0);

    //
    public int DeployedAppInstCount => AppInsts.Sum(inst => inst.IsDeployed ? 1 : 0);
    public bool AllAppInstDeployed => AppInsts.Length == DeployedAppInstCount;

    public List<AppInst> UndeployedAppInst => AppInsts.Where(inst => !inst.IsDeployed).ToList();
    //

    //
    public double TotalScore => AllAppInstDeployed && AllJobDeployed ? ActualScore : 1e9;
    public double ActualScore => Machines.Sum(m => m.Score);

    public string ScoreMsg => $"TotalScore: {TotalScore:0.00} of " +
                              $"{MachineCountHasApp} | {MachineCountHasJob} machines[App|Job] = " +
                              $"[{ActualScore / MachineCountHasApp:0.00}] ; " +
                              $"AllDeployed[App|Job]? {AllAppInstDeployed} | {AllJobDeployed} " +
                              $"UndeployedAppInst: {UndeployedAppInst.Count}";

    public void ClearAppDeploy() {
      Machines.ForEach(m => m.ClearApps());
    }

    //仅处理在线应用
    public static void SaveAndJudgeApp(Solution final) {
      var csvSubmit = $"submit_{final.DataSet.Id}.csv";
      var writer = File.CreateText(csvSubmit);

      var clone = final.DataSet.InitSolution.Clone();
      SaveAppSubmit(final, clone, writer);
      writer.Close();
      WriteLine($"== DataSet {final.DataSet.Id} Judge==");

      clone.SetInitAppDeploy();

      ReadAppSubmit(csvSubmit, clone);
      Write($"[JudgeSubmit]: {clone.ScoreMsg}");
      FinalCheckApp(clone);
    }

    // 不是每个 Solution 对象都可以读入 submit，所以用静态方法
    // 注意：会修改 clone，将其重置为初始状态，之后读入 submit

    private static void ReadAppSubmit(string csvSubmit, Solution clone,
      bool byRound = true, bool verbose = false) {
      if (byRound) { // 兼容：分轮次迁移
        ReadAppSubmitByRound(csvSubmit, clone, verbose);
      } else {
        var failedCntResource = 0;
        var failedCntX = 0;
        var lineNo = 0;
        Util.ReadCsv(csvSubmit, parts => {
          if (parts.Length == 1 && parts[0] == "#") {
            return false;
          }

          if (failedCntResource + failedCntX > 0 && !verbose) {
            return false;
          }

          var instId = parts[0].Id();
          var mId = parts[1].Id();
          var inst = clone.AppInstKv[instId];
          var m = clone.MachineKv[mId];
          lineNo++;
          inst.Machine?.Remove(inst);
          if (!m.TryPut(inst)) {
            if (m.IsOverCapacityWith(inst)) {
              failedCntResource++;
            }

            if (m.IsConflictWith(inst)) {
              failedCntX++;
            }

            Write($"[{lineNo}] ");
            Write(m.FailureMsg(inst));
            WriteLine($"\t{inst}  {m}");
          }

          return true;
        });
      }
    }

    private static void ReadAppSubmitByRound(string csvSubmit, Solution clone, bool verbose) {
      var failedCntResource = 0;
      var failedCntX = 0;
      var lineNo = 0;
      var preRound = int.MinValue;

      var pendingSet = new HashSet<AppInst>(1000);

      Util.ReadCsv(csvSubmit, parts => {
        if (parts.Length == 1 && parts[0] == "#") {
          return false;
        }

        if (failedCntResource + failedCntX > 0 && !verbose) {
          return false;
        }

        var round = int.Parse(parts[0]);

        var inst = clone.AppInstKv[parts[1].Id()];
        var m = clone.MachineKv[parts[2].Id()];

        if (preRound == int.MinValue) {
          preRound = round;
        } else if (preRound != round) { // 开始新一轮，释放上一轮迁移的机器
          preRound = round;
          foreach (var i in pendingSet) {
            i.PreMachine?.Remove(i, false);
          }

          pendingSet.Clear();
        }

        lineNo++;

        if (m.TryPut(inst, autoRemove: false)) {
          pendingSet.Add(inst);
        } else {
          if (m.IsOverCapacityWith(inst)) {
            failedCntResource++;
          }

          if (m.IsConflictWith(inst)) {
            failedCntX++;
          }

          Write($"[ReadSubmitByRound]: L{lineNo}@Round{round}");
          Write(m.FailureMsg(inst));
          WriteLine($"\t{inst}\t{m}");
        }

        return true;
      });
    }

    public static bool FinalCheckApp(Solution final, bool verbose = false) {
      var ok = true;
      if (!final.AllAppInstDeployed) {
        WriteLine(
          $"[FinalCheck]: UndeployedInst {final.DataSet.AppInstCount - final.DeployedAppInstCount}");
        return false;
      }

      foreach (var m in final.Machines) {
        if (m.IsOverCapacity) {
          Write("[FinalCheck]: OverCapacity ");
          WriteLine(m);
          ok = false;
          if (!verbose) {
            return false;
          }
        }

        foreach (var x in m.ConflictList) {
          WriteLine($"[FinalCheck]: Conflict m_{m.Id}," +
                    $"[app_{x.Item1.Id},app_{x.Item2.Id}," +
                    $"{x.Item3} > k={x.Item4}]");
          ok = false;
          if (!verbose) {
            return false;
          }
        }
      }

      return ok;
    }

    #endregion
  }
}
