using System;
using System.Collections.Generic;
using System.Linq;

namespace Tianchi {
  public class MachineType {
    private const int Ts1470 = Resource.Ts1470;

    public static int CapDiskLarge;

    public static readonly Dictionary<string, MachineType> Kv =
      new Dictionary<string, MachineType>(5);

    public bool IsLargeMachine => CapDisk == CapDiskLarge;

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
        new Series(Ts1470, mt.CapCpu),
        new Series(Ts1470, mt.CapMem),
        mt.CapDisk,
        int.Parse(fields[3]),
        int.Parse(fields[4]),
        int.Parse(fields[5])
      );
      return mt;
    }

    public static MachineType Get(string type) {
      if (!Kv.TryGetValue(type, out var mt)) {
        mt = Parse(type.Split(','));
        Kv[type] = mt;
      }

      return mt;
    }
  }

  public partial class Machine {
    private readonly bool _isAlpha10; //初赛和复赛成本分数的alpha系数不同

    // 内部状态，随着实例部署动态加减各维度资源的使用量，
    // 不必每次都对整个实例列表求和
    private readonly Resource _usage = new Resource();

    // 分应用汇总的实例个数
    public readonly Dictionary<App, int> AppCountKv = new Dictionary<App, int>();

    public readonly HashSet<AppInst> AppInstSet = new HashSet<AppInst>(20);
    public readonly Dictionary<App, AppInst> AppKv = new Dictionary<App, AppInst>();
    public readonly int Id;

    private double _score = double.MinValue;

    public double CpuUtilLimit; //检查资源约束可以限制Cpu Util
    public int InstCount { get; private set; }

    public MachineType T { get; }

    public bool IsFull => Avail.Disk < 40.0
                          || Avail.Mem.Max < 1.0
                          || Avail.Cpu.Max < 0.5; //出现的最小的资源值，同适用于DataSet A和B

    public bool IsIdle => InstCount == 0;

    // 机器按时间T平均后的成本分数
    public double Score {
      get {
        // ReSharper disable once CompareOfFloatsByEqualityOperator
        if (_score == double.MinValue) {
          if (IsIdle) {
            _score = 0.0;
            return _score;
          }

          _score = _isAlpha10 ? _usage.Cpu.Score(CapCpu) : _usage.Cpu.Score(CapCpu, InstCount);
        }

        return _score;
      }
    }

    #region 检查整体约束

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

    #endregion

    #region 添加删除在线应用实例

    // 如果添加成功，会自动从旧机器上迁移过来（如果有的话）
    public bool TryPutAppInst(AppInst inst, double cpuUtilLimit = 1.0, bool ignoreCheck = false,
      bool autoRemove = true) {
      if (AppInstSet.Contains(inst)) return true; //已经存在inst了，幂等

      if (!ignoreCheck && !CanPutInst(inst, cpuUtilLimit)) return false;

      if (!AppInstSet.Add(inst)) throw new Exception($"[TryPutInst]: {inst}");

      InstCount += 1;
      _usage.Add(inst.R);

      _score = double.MinValue;
      _avail.Invalid();
      _xUsage.Invalid();

      AppCountKv[inst.App] = AppCountKv.GetValueOrDefault(inst.App, 0) + 1;

      // 每类App只需保存一个inst作为代表即可
      AppKv.TryAdd(inst.App, inst);

      if (autoRemove) {
        // inst之前已经部署到某台机器上了，需要迁移
        inst.Machine?.RemoveAppInst(inst);
        inst.Machine = this;
      } else {
        // inst 同时占据 preMachine（可能为 null） 和 this 的资源，
        // 需手动移除
        inst.PreMachine = inst.Machine;
        inst.Machine = this;
      }

      inst.Deployed = true;

      return true;
    }

    public void RemoveAppInst(AppInst inst, bool updateDeployFlag = true) {
      if (!AppInstSet.Remove(inst)) return;

      InstCount -= 1;

      _usage.Subtract(inst.R);

      _score = double.MinValue;
      _avail.Invalid();
      _xUsage.Invalid();

      AppCountKv[inst.App] -= 1;
      if (AppCountKv[inst.App] == 0) {
        AppCountKv.Remove(inst.App);
        AppKv.Remove(inst.App);
      } else if (AppKv[inst.App] == inst) {
        //要移除的 inst 恰好是该类 App 的代表，
        //移除后需要找一个替补，而且计数不为0，表明肯定存在替补
        var found = false;
        foreach (var i in AppInstSet)
          if (i.App == inst.App) {
            AppKv[i.App] = i;
            found = true;
            break;
          }

        if (!found) throw new Exception($"[RemoveInst]: {inst}");
      }

      // 如果是从 PreMachine 调用的，不修改下面这两个字段
      if (updateDeployFlag) {
        inst.Machine = null;
        inst.Deployed = false;
      }
    }

    public void ClearAppInstSet() {
      var instList = AppInstSet.ToList();
      var len = instList.Count;
      for (var i = 0; i < len; i++) RemoveAppInst(instList[i]);
    }

    public bool CanPutInst(AppInst inst, double cpuUtilLimit = 1.0) {
      return !IsOverCapWithAppInst(inst, cpuUtilLimit) && !IsConflictWithAppInst(inst);
    }


    // 检查当前累积使用的资源量 usage **加上r之后** 是否会超出 capacity，
    // 不会修改当前资源量
    public bool IsOverCapWithAppInst(AppInst inst, double cpuUtilLimit = 1.0) {
      var r = inst.R;

      return _usage.Disk + r.Disk > CapDisk
             || _usage.P + r.P > Capacity.P
             || _usage.Pm + r.Pm > Capacity.Pm
             || _usage.M + r.M > Capacity.M
             || _usage.Cpu.MaxWith(r.Cpu) > CapCpu * cpuUtilLimit //TODO: Round? 
             || _usage.Mem.MaxWith(r.Mem) > CapMem;
    }

    // 检查App间的冲突
    public bool IsConflictWithAppInst(AppInst inst) {
      var appB = inst.App;
      var appBCnt = AppCountKv.GetValueOrDefault(appB, 0);

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

    #endregion

    #region 构造，解析，克隆

    // ReSharper disable once SuggestBaseTypeForParameter
    private Machine(string str, bool isAlpha10) {
      var i = str.IndexOf(',');
      Id = str.Substring(0, i).Id();

      T = MachineType.Get(str.Substring(i + 1));
      InstCount = 0;
      _isAlpha10 = isAlpha10;
    }

    private Machine(int id, MachineType type, bool isAlpha10) {
      Id = id;
      T = type;
      InstCount = 0;
      _isAlpha10 = isAlpha10;
    }

    public Machine Clone() {
      return new Machine(Id, T, _isAlpha10);
    }

    public static Machine Parse(string str, bool isAlpha10) {
      return new Machine(str, isAlpha10);
    }

    #endregion

    #region 辅助

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

    //返回给外部，隔离外部对剩余资源的意外修改
    private readonly Resource _avail = new Resource().Invalid();

    //返回给外部，隔离外部对累积资源的意外修改
    private readonly Resource _xUsage = new Resource().Invalid();

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

    public override string ToString() {
      return $"{CapDisk},machine_{Id},{Score:0.0}," +
             $"{Avail.Cpu.Min:0.0},{100 * UtilCpuAvg:0.0}%,{100 * UtilCpuMax:0.0}%," + //cpu
             $"{Avail.Mem.Min:0.0},{100 * UtilMemAvg:0.0}%,{100 * UtilMemMax:0.0}%," + //mem
             $"{Avail.Disk:0},{100 * UtilDisk:0.0}%," + //disk
             $"{Avail.P:0}," + //P
             $"{AppInstSet.Count},\"[{AppInstSet.ToStr(i => "inst_" + i.Id)}]\"";
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
             $"{{{AppInstSet.ToStr(i => i.R.Disk)}}} " +
             $"({InstsToStr()})";
    }

    public string InstsToStr() {
      return $"{AppInstSet.ToStr(i => $"inst_{i.Id}")}";
    }

    public string AppsToStr() {
      return $"{AppInstSet.ToStr(i => $"app_{i.App.Id}")}";
    }

    public string FailedReason(AppInst inst) {
      return $"inst_{inst.Id},m_{Id}" +
             $"{(IsOverCapWithAppInst(inst) ? ",R" : "")}" +
             $"{(IsConflictWithAppInst(inst) ? ",X" : "")}";
    }

    #endregion
  }
}