using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Tianchi {
  public class Solution {
    private readonly Dictionary<int, Instance> _instKv;

    private readonly Dictionary<int, Machine> _machineKv;
    public readonly DataSet DataSet;
    public readonly Instance[] Instances;

    public readonly Machine[] Machines;

    public StreamWriter Writer;

    public Solution(DataSet ds, StreamWriter writer = null) {
      DataSet = ds;
      Writer = writer;
      Machines = new Machine[MachineCount];
      Instances = new Instance[InstCount];
      _machineKv = new Dictionary<int, Machine>(MachineCount);
      _instKv = new Dictionary<int, Instance>(InstCount);
    }

    public int InstCount => DataSet.InstCount;
    public int MachineCount => DataSet.MachineCount;

    public int UsedMachineCount => Machines.Sum(m => m.IsIdle ? 0 : 1);
    public int DeployedInstCount => Instances.Sum(inst => inst.Deployed ? 1 : 0);
    public bool AllInstDeployed => Instances.Length == DeployedInstCount;

    public double TotalScore => AllInstDeployed ? ActualScore : 1e9;
    public double ActualScore => Machines.Sum(m => m.Score);

    public string ScoreResult => $"TotalScore: {TotalScore:0.00} of " +
                                 $"{UsedMachineCount} machines = " +
                                 $"[{ActualScore / UsedMachineCount:0.00}] ; " +
                                 $"UndeployedInst: {UndeployedInst.Count}";

    public List<Instance> UndeployedInst => Instances.Where(inst => !inst.Deployed).ToList();

    public Machine GetMachine(int id) {
      return _machineKv[id];
    }

    public Instance GetInst(int id) {
      return _instKv[id];
    }

    public Solution Clone(bool withInitDeploy = true) {
      var clone = new Solution(DataSet);
      Machines.ForEach((m, i) => clone.Machines[i] = m.Clone());
      Instances.ForEach((inst, i) => clone.Instances[i] = inst.Clone());
      clone.SetKv();

      if (withInitDeploy) clone.CloneDeloy(this);

      return clone;
    }

    public void Read(string machineCsv, string instCsv, bool withInitDeploy = true) {
      ReadMachine(machineCsv);
      ReadInstance(instCsv);
      SetKv();
      if (withInitDeploy) ReadInitDeploy();
    }

    private void SetKv() {
      Machines.ForEach(m => _machineKv[m.Id] = m);
      Instances.ForEach(inst => _instKv[inst.Id] = inst);
    }

    private void CloneDeloy(Solution original) {
      original
        .Instances
        .Where(inst => inst.Machine != null)
        .ForEach(inst => {
          var cloneM = GetMachine(inst.Machine.Id); //Id相同，但object不同
          var cloneInst = GetInst(inst.Id);
          cloneM.TryPutInst(cloneInst, ignoreCheck: true);
          cloneInst.Deployed = inst.Deployed;
        });
    }

    // 读取机器，并按磁盘大小（降序）和Id（升序）排序
    private void ReadMachine(string csv) {
      var list = new List<Machine>(10000);
      Ext.ReadCsv(csv, line => {
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
    private void ReadInstance(string csv) {
      var i = 0;
      Ext.ReadCsv(csv, fields => {
          var instId = fields[0].Id();
          var appId = fields[1].Id();

          var app = DataSet.AppKv[appId];
          var inst = new Instance(instId, app);

          app.InstCount++;

          Instances[i++] = inst;
        }
      );
    }

    //读取数据集的初始部署
    //注意：需事先清空已有部署
    //为方便调用，无需传入csv文件名
    public void ReadInitDeploy() {
      Ext.ReadCsv(DataSet.InstCsv, fields => {
          var mId = fields[2].Id();

          //可能初始状态没有分配机器
          if (mId == int.MinValue) return;

          var m = GetMachine(mId);

          var instId = fields[0].Id();
          var inst = GetInst(instId);

          //官方的评测代码在初始化阶段忽略资源和亲和性检查，直接将 inst 添加到机器上。
          //在评价阶段，如果提交的代码中有 inst 的新部署目标，才会将其迁移；
          //在迁移之前向旧机器放置实例，就可能存在不必要的资源或亲和性冲突；
          //TODO: 早先版本初赛数据集B的初始部署错误地使用了 cpuUtilLimit 检查，
          //这里的限值应该是 1.0
          var canPut = m.CanPutInst(inst);
          m.TryPutInst(inst, ignoreCheck: true);

          //因为 TryPutInst 函数会将 Deployed 置为true，
          //而且添加inst后改变了机器状态，会增加资源使用和App个数，
          //所以先保存判断结果，在机器上部署了实例后再修正 Deployed 标志
          //TODO: 早先版本存在没有事先保存判断结果的Bug，反而获得了更好的分数
          //可能是因为迁移了那些负载较重机器上的实例，这里不保持这个Bug
          inst.Deployed = canPut; //m.CanPutInst(inst); //
        }
      );
    }

    public void ClearDeploy() {
      Machines.ForEach(m => m.ClearInstSet());
    }

    public void JudgeSubmit(string csvSubmit, bool verbose = false) {
      if (!File.Exists(csvSubmit)) {
        Console.WriteLine($"Error: Can not Find Submit File {csvSubmit}!");
        return;
      }

      ClearDeploy();
      ReadInitDeploy(); //恢复初始状态

      ReadSubmit(csvSubmit, verbose);

      PrintScore();
      FinalCheck(verbose);
    }

    private void ReadSubmit(string csvSubmit, bool verbose = false) {
      Writer?.Close();

      var failedCntResource = 0;
      var failedCntX = 0;

      var lineNo = 0;

      Ext.ReadCsv(csvSubmit, fields => {
        if (failedCntResource + failedCntX > 0 && !verbose) return;
        var instId = fields[0].Id();
        var mId = fields[1].Id();
        var inst = GetInst(instId);
        var m = GetMachine(mId);
        lineNo++;
        inst.Machine?.RemoveInst(inst);

        if (!m.TryPutInst(inst)) {
          if (m.IsOverCapWithInst(inst)) failedCntResource++;

          if (m.HasConflictWithInst(inst)) failedCntX++;

          Console.Write($"[{lineNo}] ");
          Console.Write(m.FailedReason(inst));
          Console.WriteLine($"\t{inst}  {m}");
        }
      });
    }

    public bool FinalCheck(bool verbose = false) {
      var ok = true;
      if (!AllInstDeployed) {
        Console.WriteLine($"Deployed insts {DeployedInstCount} of [{DataSet.InstCount}]");
        return false;
      }

      foreach (var m in Machines) {
        if (m.IsOverCapacity) {
          Console.WriteLine(m);
          ok = false;
          if (!verbose) return false;
        }

        foreach (var x in m.ConflictList) {
          Console.WriteLine($"m_{m.Id}," +
                            $"[app_{x.Item1.Id},app_{x.Item2.Id}," +
                            $"{x.Item3} > k={x.Item4}]");
          ok = false;
          if (!verbose) return false;
        }
      }

      return ok;
    }

    public void PrintScore() {
      Console.WriteLine(ScoreResult);
    }

    public void PrintUndeployedInst() {
      UndeployedInst.ForEach(Console.WriteLine);
    }
  }
}