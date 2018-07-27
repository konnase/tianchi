using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    private const int AppCount = 9338;
    private const int InstCount = 68224; //b //68219; a
    private const int MachineCount = 6000;
    public static readonly Dictionary<int, App> AppKv = new Dictionary<int, App>(AppCount);

    public static readonly Dictionary<int, Machine> MachineKv = new Dictionary<int, Machine>(MachineCount);
    public static readonly List<Machine> Machines = new List<Machine>(6000);

    public static readonly Instance[] Instances = new Instance[InstCount];

    public static readonly Dictionary<int, Instance> InstanceKv = new Dictionary<int, Instance>(InstCount);

    private static string DataPath => $"{_projectPath}/data/";

    private static readonly string[] DataSetA = new[] {
      "scheduling_preliminary_app_resources_20180606.csv",
      "scheduling_preliminary_app_interference_20180606.csv",
      "scheduling_preliminary_machine_resources_20180606.csv",
      "scheduling_preliminary_instance_deploy_20180606.csv"
    };

    private static readonly string[] DataSetB = new[] {
      "scheduling_preliminary_b_app_resources_20180726.csv",
      "scheduling_preliminary_b_app_interference_20180726.csv",
      "scheduling_preliminary_b_machine_resources_20180726.csv",
      "scheduling_preliminary_b_instance_deploy_20180726.csv"
    };

    private static void ReadAllData(string[] dataSet) {
      ReadApp(DataPath + dataSet[0]);
      ReadInterference(DataPath + dataSet[1]);
      ReadMachine(DataPath + dataSet[2]);
      ReadInstance(DataPath + dataSet[3]);
      ReadInitDeployment(DataPath + dataSet[3]);
    }

    private static void ReadCsv(string csvFile, Action<string> action) {
      using (var csv = File.OpenText(csvFile)) {
        string line;
        while (null != (line = csv.ReadLine())) action(line);
      }
    }

    private static void ReadApp(string csv) {
      ReadCsv(csv,
        line => {
          var fields = line.Split(',');
          var appId = fields[0].Id();
          AppKv.Add(appId, App.Parse(fields));
        });
    }

    private static void ReadInterference(string csv) {
      ReadCsv(csv, line => {
        var fields = line.Split(',');

        var app = AppKv[fields[0].Id()];
        var otherAppId = fields[1].Id();
        var k = int.Parse(fields[2]);

        app.AddXRule(otherAppId, k);
      });
    }

    // 读取实例，并按磁盘大小和Id排序
    private static void ReadMachine(string csv) {
      var list = new List<Machine>(68300);
      ReadCsv(csv, line => {
        var fields = line.Split(',');
        var machineId = fields[0].Id();
        var m = Machine.Parse(fields);
        MachineKv.Add(machineId, m);
        list.Add(m);
      });

      var sorted = from l in list
        orderby l.CapDisk descending, l.Id
        select l;
      foreach (var l in sorted) Machines.Add(l);
    }

    // 读取实例，保持原有顺序！
    private static void ReadInstance(string csv) {
      var i = 0;
      ReadCsv(csv, line => {
          var fields = line.Split(',');
          var instId = fields[0].Id();
          var appId = fields[1].Id();

          var app = AppKv[appId];
          var inst = new Instance(instId, app);

          app.InstanceCount++;

          Instances[i++] = inst;
          InstanceKv.Add(instId, inst);
        }
      );
    }

    private static void ReadInitDeployment(string csv) {
      ReadCsv(csv, line => {
          var fields = line.Split(',');
          var mId = fields[2].Id();

          //可能初始状态没有分配机器
          if (mId == int.MinValue) return;

          var m = MachineKv[mId];

          var instId = fields[0].Id();
          var inst = InstanceKv[instId];

          //官方的评测代码在初始化阶段忽略资源和亲和性检查，直接将 inst 添加到机器上。
          //在评价阶段，如果提交的代码中有 inst 的新部署目标，才会将其迁移；
          //在迁移之前，向旧机器放置实例就可能导致资源或亲和性冲突；
          //这里和官方评测代码行为保持一致
          var needMigrate = m.IsOverCapacity(inst) || m.IsXWithDeployed(inst);
          m.AddInstance(inst, ignoreCheck: true);

          //因为AddInstance会将NeedDeployOrMigrate置为true，
          //而且添加inst后会增加资源使用和App个数，造成上面的判断过量，
          //所以先保存判断结果，添加后再赋值
          inst.NeedDeployOrMigrate = needMigrate;
        }
      );
    }
  }
}