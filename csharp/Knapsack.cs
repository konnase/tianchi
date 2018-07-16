using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    const int BinCount = 5506;
    static readonly int[][] Bins = new int[BinCount][];

    // 将所有实例按所需磁盘大小分组
    static readonly Dictionary<int, List<Instance>> InstDiskKv = new Dictionary<int, List<Instance>>(16);

    static void SetInstDiskKv() {
      foreach (var inst in Instances) {
        var key = inst.R.Disk;
        var val = InstDiskKv.GetValueOrDefault(key, new List<Instance>());
        val.Add(inst);
        InstDiskKv[key] = val;
      }

      //依次按cpu，mem逆序排序
      foreach (var kv in InstDiskKv) {
        kv.Value.Sort((a, b) => {
          var cpu = b.R.Cpu.Max.CompareTo(a.R.Cpu.Max);
          return cpu == 0 ? b.R.Mem.Max.CompareTo(a.R.Mem.Max) : cpu;
        });
      }
    }

    static void ParseBins(string binsFile) {
      var f = File.OpenText(binsFile);
      string line;
      var b = 0;
      //格式
      //"      5 total(1024): {40,167,167,650}"
      while (null != (line = f.ReadLine())) {
        line = line.TrimStart();
        var sp = line.IndexOf(' ');
        var cnt = int.Parse(line.Substring(0, sp));
        var lb = line.IndexOf('{');
        var rb = line.Length - 1;

        var csv = line.Substring(lb + 1, rb - lb - 1);
        var plan = csv.CsvToIntArray();
        Array.Sort(plan, (x, y) => y.CompareTo(x)); //逆序，将大的磁盘放到前面
        for (var i = 0; i < cnt; i++) {
          Bins[b++] = plan;
        }
      }

      Array.Sort(Bins, (x, y) => {
        var sum = y.Sum().CompareTo(x.Sum());
        return sum == 0 ? y[0].CompareTo(x[0]) : sum;
      }); //逆序，将大磁盘的方案放到前面
      f.Close();
    }

    // 根据已知按磁盘装箱得出的下限，按Disk大小，从相应分组找出一个实例放置到机器上
    static void PackBins(StreamWriter w) {
      Machine m = null;
      for (var k = 0; k < BinCount; k++) {
        var plan = Bins[k];
        m = FindMachine(plan);
        //TODO: 选机器，迁移
        //TODO: 先填小的实例，然后从大到小，替换成该类其它资源最大的实例

        foreach (var disk in plan) {
          var deployed = false;
          foreach (var inst in InstDiskKv[disk]) {
            if (inst.DeployedMachine != null) {
              m = inst.DeployedMachine;
            }

            if (inst.DeployedMachine != null || !m.AddInstance(inst, w)) continue;
            deployed = true;
            break;
          }

          if (!deployed) {
            //Console.WriteLine($"[{x++}]: {k} machine_{m.Id}({m.Capacity.Disk}), Disk:[{d}]{disk}");
          }
        }
      }
    }

    private static Machine FindMachine(int[] plan) {
      Machine m = null;
      //所谓主导实例，指方案中磁盘最大的实例
      var dominateInstList = InstDiskKv[plan[0]];
      foreach (var inst in dominateInstList) {
        if (inst.DeployedMachine != null && !inst.NeedDeployOrMigrate) {
          //如果最大磁盘对应的实例已经部署到某台机器上了，
          //而且inst不需要迁移，就在这台机器上接着部署其它实例
          m = inst.DeployedMachine;
          break;
        }


        //否则选择一台空机器
        var sumDisk = plan.Sum();
        foreach (var machine in Machines) {
          if (machine.CapDisk >= sumDisk && machine.IsIdle) {
            m = machine;
            break;
          }
        }

        if (m != null) {
          break;
        }
      }

      return m;
    }
  }
}