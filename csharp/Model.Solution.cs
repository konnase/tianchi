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
      bool withInitDeploy = true) {
      var solution = new Solution(dataSet, appInstCsv);
      solution.ReadMachine(machineCsv);
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
    public static bool TrySaveAppSubmit(Solution final, Solution clone,
      StreamWriter writer = null, int maxRound = 3) {
      // <inst, mIdDest>
      var migrateInstKv = GetDiff(final, clone, isDeploy: false);
      var deployInstKv = GetDiff(final, clone, isDeploy: true);

      WriteLine("[SaveAppSubmit]: " +
                $"{migrateInstKv.Count} to migrate; " +
                $"{deployInstKv.Count} to deploy.");

      // TODO: 排序策略
      // 只对 migrateInstKv 排序 
      var sortedMigrate = (from kv in migrateInstKv
        orderby kv.Key.Machine.Id descending
        select kv).ToList();

      // 机器只排序一次，把大型的，空闲资源多的机器排到前面
      // 用于临时腾挪
      var sortedMachines = (from m in clone.Machines
        orderby m.CapDisk descending, m.Usage.Disk, m.Id
        select m).ToList();

      var pendingInstSet = new HashSet<AppInst>(capacity: 1000);
      var failedInstKv = new Dictionary<AppInst, int>(); //为了减少GC

      var round = 1;

      // 迁移
      // 注意，round 是从1开始的，且循环的轮次比 maxRound 少一轮
      while (round < maxRound && migrateInstKv.Count > 0) {
        Migrate(clone, migrateInstKv, failedInstKv,
          pendingInstSet,
          sortedMigrate, sortedMachines, writer, round);

        // 释放了原机器上的资源，一轮就结束了
        ReleasePendingSet(pendingInstSet);
        round++;
      }

      // 循环结束了还有一轮的机会
      if (migrateInstKv.Count > 0) {
        Migrate(clone, migrateInstKv, failedInstKv,
          pendingInstSet,
          sortedMigrate, sortedMachines, writer, round);
      }

      var migrateCnt = migrateInstKv.Count;
      if (migrateCnt != 0) {
        WriteLine($"[SaveAppSubmit]: Migrating Failed {migrateCnt}#");
      }

      // 部署，只在最后一轮进行
      // 正常的话，完成迁移后，部署不会出现失败的实例，也不必处理 pendingInstSet
      Deploy(clone, deployInstKv, failedInstKv, pendingInstSet, writer, round);

      var deployCnt = deployInstKv.Count;
      if (deployCnt != 0) {
        WriteLine($"[SaveAppSubmit]: Deployment failed {deployCnt}#");
        var cnt = 0;
        foreach (var kv in deployInstKv) {
          cnt++;
          WriteLine($"[SaveAppSubmit]: {cnt} - {kv.Key}\t{clone.MachineKv[kv.Value]}");
          if (cnt == 5) {
            break;
          }
        }
      }

      return migrateCnt + deployCnt == 0;
    }

    /// <summary>
    ///   TODO: Tuning this
    /// </summary>
    public static double HighUtilThreshold = 0.8;

    private static void Migrate(Solution clone, Dictionary<AppInst, int> migrateKv,
      Dictionary<AppInst, int> failedKv, HashSet<AppInst> pendingSet,
      List<KeyValuePair<AppInst, int>> sortedMigrate, List<Machine> machines,
      StreamWriter writer, int round) {
      //
      var prevFailedCnt = int.MaxValue;

      while (true) {
        Deploy(clone, migrateKv, failedKv, pendingSet,
          writer, round, sortedMigrate);

        var failedCnt = failedKv.Count;

        // 没有迁移失败的实例，说明全部迁移成功了
        // 或者无法再处理迁移失败的实例了
        if (failedCnt == 0 || prevFailedCnt == failedCnt) {
          break;
        }

        prevFailedCnt = failedCnt;

        // failedKv 中不同实例的源机器和目标机器存在循环，导致死锁
        // 需临时迁移到其它任意的机器

        WriteLine($"[Migrate]: failedKv@r{round} {failedKv.Count}#");
        var cnt = 0;

        // 取快照，因为循环内部会修改 failedKv
        var list = failedKv.ToList();
        foreach (var kv in list) {
          var inst = kv.Key;
          var mSrc = inst.Machine;
          if (mSrc.UtilDisk < HighUtilThreshold &&
              mSrc.UtilCpuMax < HighUtilThreshold &&
              mSrc.UtilMemMax < HighUtilThreshold) {
            continue; // inst 所在机器负载不重，就不临时迁移了
          }

          var mIdDest = kv.Value;
          foreach (var m in machines) {
            if (m.Id == mIdDest || m == mSrc) {
              continue;
            }

            // First Fit 到其它机器上
            if (m.TryPut(inst, autoRemove: false)) {
              // inst 仍在 migrateInstKv 中，但要等下一轮才能继续迁移
              pendingSet.Add(inst);
              // 兼容：与初赛提交文件格式不同
              writer?.WriteLine($"{round},inst_{inst.Id},m_{m.Id}");
              failedKv.Remove(inst);
              cnt++;
              break;
            }
          }
        }

        WriteLine($"[Migrate]: failedKv@{round} {cnt}# migrated temporarily");
      }
    }

    private static void Deploy(Solution clone, Dictionary<AppInst, int> instKv,
      Dictionary<AppInst, int> failedKv, HashSet<AppInst> pendingSet,
      StreamWriter writer, int round, List<KeyValuePair<AppInst, int>> sortedKv = null) {
      //
      failedKv.Clear();

      ICollection<KeyValuePair<AppInst, int>> list;
      if (sortedKv != null) {
        list = sortedKv;
      } else {
        list = instKv.ToList(); //取快照
      }

      foreach (var kv in list) {
        var inst = kv.Key;
        if (!instKv.ContainsKey(inst) ||
            pendingSet.Contains(inst)) { //本轮已经迁移过了，等待下一轮吧
          continue;
        }

        var mIdDest = kv.Value;
        var mDest = clone.MachineKv[mIdDest];
        if (mDest.TryPut(inst, autoRemove: false)) {
          pendingSet.Add(inst);
          instKv.Remove(inst);
          // 兼容：与初赛提交文件格式不同
          writer?.WriteLine($"{round},inst_{inst.Id},m_{mDest.Id}");
        } else {
          failedKv[inst] = mIdDest;
        }
      }
    }

    private static void ReleasePendingSet(HashSet<AppInst> pendingSet) {
      foreach (var inst in pendingSet) {
        inst.PreMachine?.Remove(inst, setDeployFlag: false);
      }

      pendingSet.Clear();
    }

    // 不会修改 init
    private static Dictionary<AppInst, int> GetDiff(Solution final, Solution init, bool isDeploy) {
      var diffKv = new Dictionary<AppInst, int>();
      foreach (var instF in final.AppInsts) {
        var instI = init.AppInstKv[instF.Id];
        if (isDeploy && instI.Machine == null) {
          var mIdDest = instF.Machine.Id;
          diffKv[instI] = mIdDest;
        } else if (!isDeploy && instI.Machine != null) {
          var mIdSrc = instI.Machine.Id;
          var mIdDest = instF.Machine.Id;
          if (mIdSrc != mIdDest) {
            diffKv[instI] = mIdDest;
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
    private void ReadMachine(string csv) {
      var list = new List<Machine>(capacity: 10000);
      Util.ReadCsv(csv, line => {
        var m = Machine.Parse(line);
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

          // 因为 TryPut 函数会将 Deployed 置为true，
          // 而且添加inst后改变了机器状态，会增加资源使用和App个数，
          // 所以先保存判断结果，在机器上部署了实例后再修正 Deployed 标志
          // Fixed: 早先版本存在没有事先保存判断结果的Bug，反而获得了更好的分数
          // 可能是因为迁移了那些负载较重机器上的实例，这里不保持这个Bug
          inst.IsDeployed = canPut; //m.CanPut(inst); 
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
      TrySaveAppSubmit(final, clone, writer);
      writer.Close();
      WriteLine($"== DataSet {final.DataSet.Id} Judge==");

      clone.SetInitAppDeploy();

      ReadAppSubmit(clone, csvSubmit);
      Write($"[SaveAndJudgeApp]: {clone.ScoreMsg}");
      CheckAppInterference(clone);
      CheckResource(clone);
    }

    // 不是每个 Solution 对象都可以读入 submit，所以用静态方法
    // 注意：会修改 clone，将其重置为初始状态，之后读入 submit

    public static void ReadAppSubmit(Solution clone, string csvSubmit,
      bool byRound = true, bool verbose = false) {
      if (byRound) { // 兼容：分轮次迁移
        ReadAppSubmitByRound(clone, csvSubmit, verbose);
      } else {
        var failedCntResource = 0;
        var failedCntX = 0;
        var lineNo = 0;
        Util.ReadCsv(csvSubmit, parts => {
          if (parts.Length == 4) {
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

    private static void ReadAppSubmitByRound(Solution clone, string csvSubmit, bool verbose) {
      var failedCntResource = 0;
      var failedCntX = 0;
      var lineNo = 0;
      var prevRound = int.MinValue;

      var pendingSet = new HashSet<AppInst>(capacity: 5000);
      var curRoundInstSet = new HashSet<AppInst>(capacity: 5000);

      Util.ReadCsv(csvSubmit, parts => {
        if (parts.Length == 4) { // Jobs 有4个字段
          return false;
        }

        if (failedCntResource + failedCntX > 0 && !verbose) {
          return false;
        }

        var round = int.Parse(parts[0]);

        // 开始新一轮，释放上一轮迁移的机器
        if (prevRound != round) {
          prevRound = round;
          foreach (var i in pendingSet) {
            i.PreMachine?.Remove(i, setDeployFlag: false);
          }

          pendingSet.Clear();
          curRoundInstSet.Clear();
        }

        lineNo++;

        var inst = clone.AppInstKv[parts[1].Id()];
        var m = clone.MachineKv[parts[2].Id()];

        if (curRoundInstSet.Contains(inst)) {
          WriteLine($"[ReadAppSubmitByRound]: L{lineNo}@r{round} " +
                    "multiple deployment in the same round." +
                    $"\t{inst}\t{m}");
          return false;
        }

        curRoundInstSet.Add(inst);

        if (m.TryPut(inst, autoRemove: false)) {
          pendingSet.Add(inst);
        } else {
          if (m.IsOverCapacityWith(inst)) {
            failedCntResource++;
          }

          if (m.IsConflictWith(inst)) {
            failedCntX++;
          }

          Write($"[ReadAppSubmitByRound]: L{lineNo}@r{round} ");
          Write(m.FailureMsg(inst));
          WriteLine($"\t{inst}\t{m}");
        }

        return true;
      });

      foreach (var i in pendingSet) {
        i.PreMachine?.Remove(i, setDeployFlag: false);
      }
    }

    // 如果正常，不输出信息
    public static bool CheckAppInterference(Solution final, bool verbose = false) {
      var ok = true;
      foreach (var m in final.Machines) {
        foreach (var x in m.ConflictList) {
          WriteLine($"[CheckAppInterference]: Conflict m_{m.Id}," +
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

    // 部署了App和Job之后，使用资源都不能超额
    public static bool CheckResource(Solution final, bool verbose = false) {
      var ok = true;
      foreach (var m in final.Machines) {
        if (m.IsOverCapacity) {
          Write("[CheckResource]: OverCapacity ");
          WriteLine(m);
          m.BatchKv.ForEach(kv => Write($"{kv.Value.Task.FullId},"));
          WriteLine();
          ok = false;
          if (!verbose) {
            return false;
          }
        }
      }

      return ok;
    }

    public static bool CheckAllDeployed(Solution final) {
      var appOk = final.AllAppInstDeployed;
      var jobOk = final.AllJobDeployed;
      if (!appOk || !jobOk) {
        WriteLine("[CheckAllDeployed]: " +
                  $"Apps {final.AllAppInstDeployed}, " +
                  $"Jobs {final.AllJobDeployed}");
      }

      return appOk && jobOk;
    }

    #endregion
  }
}
