using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Tianchi {
  public class Machine {
    private const int TsCount = Resource.TsCount;

    private readonly Resource _avail = new Resource().Invalid(); //返回给外部，隔离外部对累积资源的意外修改

    // 内部状态，随着实例部署动态加减各维度资源的使用量，
    // 不必每次都对整个实例列表求和
    private readonly Resource _usage = new Resource();
    private readonly Resource _xUsage = new Resource().Invalid(); //返回给外部，隔离外部对累积资源的意外修改

    // 分应用汇总的实例个数
    public readonly Dictionary<App, int> AppCountKv = new Dictionary<App, int>();
    public readonly Dictionary<App, Instance> AppInstKv = new Dictionary<App, Instance>();
    public readonly Resource Capacity;
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
      Capacity = new Resource(
        new Series(TsCount, CapCpu),
        new Series(TsCount, CapMem),
        CapDisk,
        int.Parse(fields[4]),
        int.Parse(fields[5]),
        int.Parse(fields[6])
      );
      IsIdle = true;
      CpuUtilThreshold = Math.Abs(CapCpu - 92) < 0.01 ? Program.UtilCpuH : Program.UtilCpuL;
    }

    public static double CpuUtilThreshold { get; private set; }

    public double UtilCpuAvg => _usage.Cpu.Avg / CapCpu;
    public double UtilCpuMax => _usage.Cpu.Max / CapCpu;
    public double UtilMemAvg => _usage.Mem.Avg / CapMem;
    public double UtilMemMax => _usage.Mem.Max / CapMem;
    public double UtilDisk => _usage.Disk * 1.0 / CapDisk;

    public Resource Avail {
      get {
        if (!_avail.IsValid) _avail.SubtractByCapacity(Capacity, _usage);

        return _avail;
      }
    }

    public Resource Usage {
      get {
        if (!_xUsage.IsValid) _xUsage.Copy(_usage);

        return _xUsage;
      }
    }

    public bool IsFull => Avail.Disk < 40.0
                          || Avail.Mem.Max < 1.0
                          || Avail.Cpu.Max < 0.5; //出现的最小的资源值，同适用于DataSet A和B

    public bool IsIdle { get; private set; }

    // 机器按时间T平均后的成本分数
    public double Score {
      get {
        // ReSharper disable once CompareOfFloatsByEqualityOperator
        if (_score == double.MinValue) {
          if (IsIdle) {
            _score = 0.0;
            return _score;
          }

          _score = _usage.Cpu.Score(CapCpu);
        }

        return _score;
      }
    }

    public bool IsOverCapacity =>
      Math.Round(Avail.Cpu.Min, 8) < 0
      || Math.Round(Avail.Mem.Min, 8) < 0
      || Avail.Disk < 0
      || Avail.P < 0
      || Avail.M < 0
      || Avail.Pm < 0;

    public bool HasConflict {
      get {
        foreach (var kv in AppCountKv) {
          var appB = kv.Key;
          var appBCnt = kv.Value;
          foreach (var ku in AppCountKv) {
            var appA = ku.Key;
            //因为遍历了两遍app列表，所以这里只需单向检测即可
            if (appBCnt > appA.XLimit(appB)) return true;
          }
        }

        return false;
      }
    }

    public List<Tuple<App, App, int, int>> ConflictList {
      get {
        var list = new List<Tuple<App, App, int, int>>();
        foreach (var kv in AppCountKv) {
          var appB = kv.Key;
          var appBCnt = kv.Value;
          foreach (var ku in AppCountKv) {
            var appA = ku.Key;
            //因为遍历了两遍app列表，所以这里只需单向检测即可
            var appBLimit = appA.XLimit(appB);
            if (appBCnt > appBLimit)
              list.Add(new Tuple<App, App, int, int>(appA, appB, appBCnt, appBLimit));
          }
        }

        return list;
      }
    }

    // 如果添加成功，会自动从旧机器上迁移过来（如果有的话）
    public bool TryPutInst(Instance inst, StreamWriter w = null,
      bool ignoreCheck = false, bool fullCapacity = false) {
      if (InstList.Contains(inst)) { //幂等
        Debug.Assert(inst.Machine == this);
        return true;
      }

      if (!ignoreCheck && (IsOverCapWithInst(inst, fullCapacity)
                           || HasConflictWithInst(inst))) return false;

      InstList.Add(inst);
      IsIdle = false;
      _usage.Add(inst.R);

      _score = double.MinValue;
      _avail.Invalid();
      _xUsage.Invalid();

      AppCountKv[inst.App] = AppCountKv.GetValueOrDefault(inst.App, 0) + 1;

      if (!AppInstKv.ContainsKey(inst.App)) AppInstKv[inst.App] = inst;

      //inst之前已经部署到某台机器上了，需要迁移
      inst.Machine?.RemoveInst(inst);
      inst.Machine = this;

      inst.NeedDeployOrMigrate = false;

      w?.WriteLine($"inst_{inst.Id},machine_{Id}");
      return true;
    }

    public void RemoveInst(Instance inst) {
      if (inst.Machine != this) return;

      InstList.Remove(inst);

      if (InstList.Count == 0) IsIdle = true;

      _usage.Subtract(inst.R);

      _score = double.MinValue;
      _avail.Invalid();
      _xUsage.Invalid();

      AppCountKv[inst.App] -= 1;
      if (AppCountKv[inst.App] == 0) {
        AppCountKv.Remove(inst.App);
        AppInstKv.Remove(inst.App);
      }

      inst.Machine = null;
      inst.NeedDeployOrMigrate = true;
    }

    public void ClearAllInsts() {
      for (var i = InstList.Count - 1; i >= 0; i--) RemoveInst(InstList[i]);
    }

    public bool CanPutInst(Instance inst, bool fullCapacity = false) {
      return !IsOverCapWithInst(inst, fullCapacity) && !HasConflictWithInst(inst);
    }


    // 检查当前累积使用的资源量 usage **加上r之后** 是否会超出 capacity，
    // 不会修改当前资源量
    public bool IsOverCapWithInst(Instance inst, bool fullCapacity = false) {
      //DataSetB only!!!
      var r = inst.R;
      var factor = fullCapacity ? 1.0 : CpuUtilThreshold;

      return _usage.Disk + r.Disk > CapDisk
             || _usage.P > Capacity.P
             || _usage.Pm + r.Pm > Capacity.Pm
             || _usage.M > Capacity.M
             || _usage.Cpu.MaxWith(r.Cpu) > factor * CapCpu //TODO: Round
             || _usage.Mem.MaxWith(r.Mem) > CapMem;
    }

    // 检查App间的冲突
    public bool HasConflictWithInst(Instance inst) {
      var appB = inst.App;

      var appBCnt = AppCountKv.ContainsKey(appB) ? AppCountKv[appB] : 0;

      foreach (var kv in AppCountKv) {
        //<appA, appB, k>
        var appA = kv.Key; //已部署的应用
        var bLimit = appA.XLimit(appB);

        if (appBCnt + 1 > bLimit)
          return true;

        //同时，已部署的应用不会与将要部署的inst的规则冲突
        //<appB,appA,aLimit>
        var aLimit = appB.XLimit(appA);
        var appACnt = kv.Value;

        if (appACnt > aLimit)
          return true;
      }

      return false;
    }

    public static Machine Parse(string[] fields) {
      return new Machine(fields);
    }

    public override string ToString() {
      return $"{CapDisk},machine_{Id},{Score:0.0}," +
             $"{Avail.Cpu.Min:0.0},{100 * UtilCpuAvg:0.0}%,{100 * UtilCpuMax:0.0}%," + //cpu
             $"{Avail.Mem.Min:0.0},{100 * UtilMemAvg:0.0}%,{100 * UtilMemMax:0.0}%," + //mem
             $"{Avail.Disk:0},{100 * UtilDisk:0.0}%," + //disk
             $"{Avail.P:0}," + //P
             $"{InstList.Count},\"[{InstList.ToStr(i => i.R.Disk)}]\"";
    }

    public string ToSearchStr() {
      if (IsIdle) return string.Empty;

      InstList.Sort((i, j) => j.R.Disk.CompareTo(i.R.Disk));
      return $"total({Score:0.00},{UtilCpuMax:0.00},{_usage.Disk}): " +
             $"{{{InstList.ToStr(i => i.R.Disk)}}} " +
             $"({InstListToStr()})";
    }

    public string InstListToStr() {
      return $"{InstList.ToStr(i => $"inst_{i.Id}")}";
    }

    public string AppListToStr() {
      return $"{InstList.ToStr(i => $"app_{i.App.Id}")}";
    }

    public string FailedReason(Instance inst) {
      return $"inst_{inst.Id},m_{Id}" +
             $"{(IsOverCapWithInst(inst, true) ? ",R" : "")}" +
             $"{(HasConflictWithInst(inst) ? ",X" : "")}";
    }
  }
}