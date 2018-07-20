using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Tianchi {
  public class Machine {
    private const int TsCount = Resource.TsCount;

    private const double Alpha = 10, Beta = 0.5;

    // 内部状态，随着实例部署动态加减各维度资源的使用量，
    // 不必每次都对整个实例列表求和
    private readonly Resource _accum = new Resource();
    private readonly Resource _avail = new Resource();

    // 分应用汇总的实例个数
    public readonly Dictionary<App, int> AppCount = new Dictionary<App, int>();
    public readonly Resource Cap;
    public readonly double CapCpu;
    public readonly int CapDisk;
    public readonly double CapMem;

    public readonly int Id;

    //用Set更合适，但仍要保证添加实例的幂等性
    public readonly List<Instance> InstList = new List<Instance>();

    private double _score = double.MinValue;

    // ReSharper disable once SuggestBaseTypeForParameter
    private Machine(string[] fields) {
      Id = fields[0].Id();
      CapCpu = double.Parse(fields[1]);
      CapMem = double.Parse(fields[2]);
      CapDisk = int.Parse(fields[3]);
      Cap = new Resource(
        new Series(TsCount, CapCpu),
        new Series(TsCount, CapMem),
        CapDisk,
        int.Parse(fields[4]),
        int.Parse(fields[5]),
        int.Parse(fields[6])
      );
      IsIdle = true;
    }

    public double UtilCpuAvg => _accum.Cpu.Avg / CapCpu;
    public double UtilCpuMax => _accum.Cpu.Max / CapCpu;
    public double UtilMemAvg => _accum.Mem.Avg / CapMem;
    public double UtilMemMax => _accum.Mem.Max / CapMem;
    public double UtilDisk => _accum.Disk * 1.0 / CapDisk;

    public Resource Avail {
      get {
        _avail.SubtractByCap(Cap, _accum);
        return _avail;
      }
    }

    public bool IsFull => Avail.Disk < 40.0
                          || Avail.Mem.Max < 1.0
                          || Avail.Cpu.Max < 0.5; //出现的最小的资源值

    public bool IsIdle { get; private set; }

    // 机器按时间T平均后的成本分数
    public double Score {
      get {
        // ReSharper disable once CompareOfFloatsByEqualityOperator
        if (_score == double.MinValue) _calcScore();

        return _score;
      }
    }

    private void _calcScore() {
      if (IsIdle) {
        _score = 0.0;
        return;
      }

      var sum = 0.0;
      for (var ts = 0; ts < TsCount; ts++) {
        var c = _accum.Cpu[ts] / CapCpu;
        sum += c <= Beta ? 1.0 : Alpha * Math.Exp(c - Beta) - Alpha + 1.0;
      }

      _score = sum / TsCount;
    }

    // 如果添加成功，会自动从旧机器上迁移过来（如果有的话）
    public bool AddInstance(Instance inst, StreamWriter w = null, bool ignoreCheck = false) {
      if (InstList.Contains(inst)) { //幂等
        Debug.Assert(inst.DeployedMachine == this);
        return true;
      }

      if (!ignoreCheck && (IsOverCapacity(inst) || IsXWithDeployed(inst))) return false;

      InstList.Add(inst);
      IsIdle = false;
      _accum.Add(inst.R);

      _score = double.MinValue;

      if (!AppCount.ContainsKey(inst.App))
        AppCount[inst.App] = 1;
      else
        AppCount[inst.App] += 1;

      //inst之前已经部署到某台机器上了，需要迁移
      inst.DeployedMachine?.RemoveInstance(inst);
      inst.DeployedMachine = this;

      inst.NeedDeployOrMigrate = false;

      w?.WriteLine($"inst_{inst.Id},machine_{Id}");
      return true;
    }

    public void RemoveInstance(Instance inst) {
      if (inst.DeployedMachine != this) return;

      InstList.Remove(inst);

      if (InstList.Count == 0) IsIdle = true;

      _accum.Subtract(inst.R);

      _score = double.MinValue;

      AppCount[inst.App] -= 1;
      if (AppCount[inst.App] == 0) AppCount.Remove(inst.App);

      inst.DeployedMachine = null;
      inst.NeedDeployOrMigrate = true;
    }

    public void ClearInstances() {
      for (var i = InstList.Count - 1; i >= 0; i--) RemoveInstance(InstList[i]);
    }

    // 检查当前累积使用的资源量 _accum **加上r之后** 是否会超出 capacity，
    // 不会修改当前资源量
    public bool IsOverCapacity(Instance inst) {
      var r = inst.R;
      return _accum.Disk + r.Disk > CapDisk
             || _accum.P + r.P > Cap.P
             || _accum.Pm + r.Pm > Cap.Pm //所有App的PM都等于P
             || _accum.M + r.M > Cap.M //所有App的M都是0
             || _accum.Cpu.MaxWith(r.Cpu) > CapCpu
             || _accum.Mem.MaxWith(r.Mem) > CapMem;
    }

    // 检查App间的冲突
    public bool IsXWithDeployed(Instance inst) {
      var appB = inst.App;

      var appBCnt = AppCount.ContainsKey(appB) ? AppCount[appB] : 0;

      foreach (var kv in AppCount) {
        //<appA, appB, k>
        var appA = kv.Key; //已部署的应用
        var k = appA.XLimit(appB);

        if (appBCnt + 1 > k)
          return true;

        //同时，已部署的应用不会与将要部署的inst的规则冲突
        //<appB,appA,xk>
        var xk = appB.XLimit(appA);
        var appACnt = kv.Value;

        if (appACnt > xk)
          return true;
      }

      return false;
    }

    public static Machine Parse(string[] fields) {
      return new Machine(fields);
    }

    public override string ToString() {
      return $"{CapDisk},{Id},{Score:0.0}," +
             $"{Avail.Cpu.Min:0},{100 * UtilCpuMax:0}%,{100 * UtilCpuAvg:0}%," + //cpu
             $"{Avail.Mem.Min:0},{100 * UtilMemMax:0}%,{100 * UtilMemAvg:0}%," + //mem
             $"{Avail.Disk:0},{100 * UtilDisk:0}%," + //disk
             $"{Avail.P:0}," + //P
             $"{InstList.Count},\"[{InstList.ToStr(i => i.R.Disk)}]\"";
    }

    public string InstListToStr() {
      return $"[{InstList.ToStr(i => $"inst_{i.Id}")}]";
    }

    public string AppListToStr() {
      return $"[{InstList.ToStr(i => $"app_{i.App.Id}")}]";
    }

    public string FailedReason(Instance inst) {
      return $"inst_{inst.Id},m_{Id}" +
             $"{(IsOverCapacity(inst) ? ",R" : "")}" +
             $"{(IsXWithDeployed(inst) ? ",X" : "")}";
    }
  }
}