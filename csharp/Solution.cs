using System;
using System.Collections.Generic;
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

    // 使 this 与 src 的部署状态保持一致，既同步已部署的实例，也同步未部署的，以及未决的
    private void CopyAppDeploy(Solution src) {
      if (src.DataSet != DataSet) {
        throw new Exception($"[CopyAppDeploy]: DataSet {DataSet.Id} mismatch!");
      }

      ClearAppDeploy();

      src.AppInsts.ForEach(instSrc => {
        var instClone = AppInstKv[instSrc.Id];
        if (instSrc.Machine != null) {
          var mClone = MachineKv[instSrc.Machine.Id]; //Id相同，但object不同
          mClone.TryPut(instClone, ignoreCheck: true);
        }

        instClone.PrevMachine = null;
        if (instSrc.PrevMachine != null) {
          instClone.PrevMachine = MachineKv[instSrc.PrevMachine.Id];
        }

        instClone.IsDeployed = instSrc.IsDeployed;
      });
    }

    public void SetInitDeploy() {
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

    #region  Read Csv

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
    public double TotalScore => AllAppInstDeployed && AllJobDeployed ? ActualScore : 1e9;

    /// <summary>
    ///   TODO: 手动控制计算成本分数的时机
    /// </summary>
    public double CalcActualScore() {
      ActualScore = Machines.Sum(m => m.Score);
      return ActualScore;
    }

    public double ActualScore { get; private set; }

    public string ScoreMsg => $"TotalScore: {TotalScore:0.00} of " +
                              $"{MachineCountHasApp} | {MachineCountHasJob} machines[App|Job] = " +
                              $"[{ActualScore / MachineCountHasApp:0.00}] ; " +
                              $"AllDeployed[App|Job]? {AllAppInstDeployed} | {AllJobDeployed} " +
                              $"UndeployedAppInst: {UndeployedAppInst.Count}";

    public void ClearAppDeploy() {
      Machines.ForEach(m => m.ClearApps());
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
