using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Tianchi {
  public class MachineType {
    private const int TsCount = Resource.TsCount;

    public static int LargeDiskCap;

    public static readonly Dictionary<string, MachineType> Kv =
      new Dictionary<string, MachineType>(5);

    public bool IsLargeMachine => CapDisk == LargeDiskCap;

    public Resource Capacity { get; private set; }
    public double CapCpu { get; private set; }
    public double CapMem { get; private set; }
    public int CapDisk { get; private set; }

    private static MachineType Parse(string[] fields) {
      var mt = new MachineType {
        CapCpu = double.Parse(fields[0]),
        CapMem = double.Parse(fields[1]),
        CapDisk = int.Parse(fields[2])
      };

      mt.Capacity = new Resource(
        new Series(TsCount, mt.CapCpu),
        new Series(TsCount, mt.CapMem),
        mt.CapDisk,
        int.Parse(fields[3]),
        int.Parse(fields[4]),
        int.Parse(fields[5])
      );
      return mt;
    }

    public static MachineType Get(string type) {
      if (Kv.ContainsKey(type)) return Kv[type];

      var mt = Parse(type.Split(','));
      Kv[type] = mt;
      return mt;
    }
  }

  public class Machine {
    //返回给外部，隔离外部对剩余资源的意外修改
    private readonly Resource _avail = new Resource().Invalid();

    // 内部状态，随着实例部署动态加减各维度资源的使用量，
    // 不必每次都对整个实例列表求和
    private readonly Resource _usage = new Resource();

    //返回给外部，隔离外部对累积资源的意外修改
    private readonly Resource _xUsage = new Resource().Invalid();

    // 分应用汇总的实例个数
    public readonly Dictionary<App, int> AppCountKv = new Dictionary<App, int>();
    public readonly Dictionary<App, Instance> AppInstKv = new Dictionary<App, Instance>();

    public readonly int Id;

    public readonly HashSet<Instance> InstSet = new HashSet<Instance>(20);

    private double _score = double.MinValue;

    public double CpuUtilLimit;

    // ReSharper disable once SuggestBaseTypeForParameter
    private Machine(string str) {
      var i = str.IndexOf(',');
      Id = str.Substring(0, i).Id();

      T = MachineType.Get(str.Substring(i + 1));
      IsIdle = true;
    }

    private Machine(int id, MachineType type) {
      Id = id;
      T = type;
      IsIdle = true;
    }

    public MachineType T { get; }

    public Resource Capacity => T.Capacity;
    public double CapCpu => T.CapCpu;
    public int CapDisk => T.CapDisk;
    public double CapMem => T.CapMem;
    public bool IsLargeMachine => T.IsLargeMachine;

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
        if (!_xUsage.IsValid) _xUsage.CopyFrom(_usage);

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
      Avail.Disk < 0
      || Avail.P < 0
      || Avail.M < 0
      || Avail.Pm < 0
      || Math.Round(Avail.Cpu.Min, 8) < 0
      || Math.Round(Avail.Mem.Min, 8) < 0;

    public bool HasConflict {
      get {
        foreach (var kv in AppCountKv) {
          var appB = kv.Key;
          var appBCnt = kv.Value;
          foreach (var ku in AppCountKv) {
            var appA = ku.Key;
            //因为遍历了两遍app列表，所以这里只需单向检测即可
            if (appBCnt > appA.XLimit(appB.Id)) return true;
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
            var appBLimit = appA.XLimit(appB.Id);
            if (appBCnt > appBLimit)
              list.Add(new Tuple<App, App, int, int>(appA, appB, appBCnt, appBLimit));
          }
        }

        return list;
      }
    }

    // 如果添加成功，会自动从旧机器上迁移过来（如果有的话）
    public bool TryPutInst(Instance inst, StreamWriter w = null,
      double cpuUtilLimit = 1.0, bool ignoreCheck = false) {
      if (!ignoreCheck && !CanPutInst(inst, cpuUtilLimit)) return false;

      if (!InstSet.Add(inst)) return true; //已经存在inst了，幂等

      IsIdle = false;
      _usage.Add(inst.R);

      _score = double.MinValue;
      _avail.Invalid();
      _xUsage.Invalid();

      AppCountKv[inst.App] = AppCountKv.GetValueOrDefault(inst.App, 0) + 1;

      //每类App只需保存一个inst作为代表即可
      if (!AppInstKv.ContainsKey(inst.App)) AppInstKv[inst.App] = inst;

      //inst之前已经部署到某台机器上了，需要迁移
      inst.Machine?.RemoveInst(inst);
      inst.Machine = this;

      inst.Deployed = true;

      w?.WriteLine($"inst_{inst.Id},machine_{Id}");
      return true;
    }

    public void RemoveInst(Instance inst) {
      if (inst.Machine != this) return;

      if (!InstSet.Remove(inst)) return;

      IsIdle = InstSet.Count == 0;

      _usage.Subtract(inst.R);

      _score = double.MinValue;
      _avail.Invalid();
      _xUsage.Invalid();

      AppCountKv[inst.App] -= 1;
      if (AppCountKv[inst.App] == 0) {
        AppCountKv.Remove(inst.App);
        AppInstKv.Remove(inst.App);
      } else if (AppInstKv[inst.App] == inst) {
        //要移除的 inst 恰好是该类 App 的代表，
        //移除后需要找一个替补，而且计数不为0，表明肯定存在替补
        var found = false;
        foreach (var i in InstSet)
          if (i.App == inst.App) {
            AppInstKv[i.App] = i;
            found = true;
            break;
          }

        if (!found) throw new Exception($"RemoveInst: app_{inst.App.Id}, inst_{inst.Id}");
      }

      inst.Machine = null;
      inst.Deployed = false;
    }

    public void ClearInstSet() {
      var instList = InstSet.ToList();
      var len = instList.Count;
      for (var i = 0; i < len; i++) RemoveInst(instList[i]);
    }

    public bool CanPutInst(Instance inst, double cpuUtilLimit = 1.0) {
      return !IsOverCapWithInst(inst, cpuUtilLimit) && !HasConflictWithInst(inst);
    }


    // 检查当前累积使用的资源量 usage **加上r之后** 是否会超出 capacity，
    // 不会修改当前资源量
    public bool IsOverCapWithInst(Instance inst, double cpuUtilLimit = 1.0) {
      var r = inst.R;

      return _usage.Disk + r.Disk > CapDisk
             || _usage.P + r.P > Capacity.P
             || _usage.Pm + r.Pm > Capacity.Pm
             || _usage.M + r.M > Capacity.M
             || _usage.Cpu.MaxWith(r.Cpu) > CapCpu * cpuUtilLimit //TODO: Round? 
             || _usage.Mem.MaxWith(r.Mem) > CapMem;
    }

    // 检查App间的冲突
    public bool HasConflictWithInst(Instance inst) {
      var appB = inst.App;
      var appBCnt = AppCountKv.ContainsKey(appB) ? AppCountKv[appB] : 0;

      foreach (var kv in AppCountKv) {
        //<appA, appB, bLimit>
        var appA = kv.Key; //已部署的应用\
        if (appA == null) continue;

        var bLimit = appA.XLimit(appB.Id);

        if (appBCnt + 1 > bLimit)
          return true;

        //同时，已部署的应用不会与将要部署的inst的规则冲突
        //<appB, appA, aLimit>
        var aLimit = appB.XLimit(appA.Id);
        var appACnt = kv.Value;

        if (appACnt > aLimit)
          return true;
      }

      return false;
    }

    public Machine Clone() {
      return new Machine(Id, T);
    }

    public static Machine Parse(string str) {
      return new Machine(str);
    }

    public override string ToString() {
      return $"{CapDisk},machine_{Id},{Score:0.0}," +
             $"{Avail.Cpu.Min:0.0},{100 * UtilCpuAvg:0.0}%,{100 * UtilCpuMax:0.0}%," + //cpu
             $"{Avail.Mem.Min:0.0},{100 * UtilMemAvg:0.0}%,{100 * UtilMemMax:0.0}%," + //mem
             $"{Avail.Disk:0},{100 * UtilDisk:0.0}%," + //disk
             $"{Avail.P:0}," + //P
             $"{InstSet.Count},\"[{InstSet.ToStr(i => "n_" + i.Id)}]\"";
    }

    //输出机器的资源占用情况
    public static void PrintList(IEnumerable<Machine> machines) {
      Console.WriteLine("cap_disk,mid,score," +
                        "avl_cpu_min,util_cpu_max,util_cpu_avg," +
                        "avl_mem_min,util_mem_max,util_mem_avg," +
                        "avl_disk,util_disk,avl_p," +
                        "inst_cnt,inst_list");
      machines.ForEach(Console.WriteLine);
    }

    public string ToSearchStr() {
      return $"total({Score:0.00},{Id},{UtilCpuMax:0.00},{_usage.Disk}): " +
             $"{{{InstSet.ToStr(i => i.R.Disk)}}} " +
             $"({InstsToStr()})";
    }

    public string InstsToStr() {
      return $"{InstSet.ToStr(i => $"inst_{i.Id}")}";
    }

    public string AppsToStr() {
      return $"{InstSet.ToStr(i => $"app_{i.App.Id}")}";
    }

    public string FailedReason(Instance inst) {
      return $"inst_{inst.Id},m_{Id}" +
             $"{(IsOverCapWithInst(inst) ? ",R" : "")}" +
             $"{(HasConflictWithInst(inst) ? ",X" : "")}";
    }
  }
}