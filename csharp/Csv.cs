using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    public static readonly Dictionary<int, App> AppKv = new Dictionary<int, App>(9338);

    public static readonly Dictionary<int, Machine> MachineKv = new Dictionary<int, Machine>(6000);
    public static readonly List<Machine> Machines = new List<Machine>(6000);

    public static readonly List<Instance> Instances = new List<Instance>(68300);
    public static readonly Dictionary<int, Instance> InstanceKv = new Dictionary<int, Instance>(68300);

    private static string DataPath => $"{_projectPath}/data/";
    private static string CsvApp => $"{DataPath}/scheduling_preliminary_app_resources_20180606.csv";
    private static string CsvDeploy => $"{DataPath}/scheduling_preliminary_instance_deploy_20180606.csv";
    private static string CsvInterference => $"{DataPath}/scheduling_preliminary_app_interference_20180606.csv";
    private static string CsvMachine => $"{DataPath}/scheduling_preliminary_machine_resources_20180606.csv";

    private static void ClearMachineDeployment() {
      foreach (var m in Machines) m.ClearInstances();

      Debug.Assert(!Instances.Exists(inst => inst.IsDeployed));
    }

    private static void ClearMachineDeployment(IEnumerable<Machine> list) {
      foreach (var m in list) m.ClearInstances();
    }

    private static void ReadAllData(bool printUndeployedInst = false) {
      ReadApp();
      ReadInterference();
      ReadMachine();
      ReadInstance();
      ReadInitDeployment(printUndeployedInst);
    }

    private static void ReadCsv(string csvFile, Action<string> action) {
      using (var csv = File.OpenText(csvFile)) {
        string line;
        while (null != (line = csv.ReadLine())) action(line);
      }
    }

    private static void ReadApp() {
      ReadCsv(CsvApp,
        line => {
          var fields = line.Split(',');
          var appId = fields[0].Id();
          AppKv.Add(appId, App.Parse(fields));
        });
    }

    private static void ReadInterference() {
      ReadCsv(CsvInterference, line => {
        var fields = line.Split(',');

        var app = AppKv[fields[0].Id()];
        var otherAppId = fields[1].Id();
        var k = int.Parse(fields[2]);

        app.AddXRule(otherAppId, k);
      });
    }

    // 读取实例，并按磁盘大小和Id排序
    private static void ReadMachine() {
      var list = new List<Machine>(68300);
      ReadCsv(CsvMachine, line => {
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

    // 读取实例，并按磁盘大小排序
    private static void ReadInstance() {
      var list = new List<Instance>(6000);
      ReadCsv(CsvDeploy, line => {
          var fields = line.Split(',');
          var instId = fields[0].Id();
          var appId = fields[1].Id();

          var app = AppKv[appId];
          var inst = new Instance(instId, app);

          app.InstanceCount++;

          list.Add(inst);
          InstanceKv.Add(instId, inst);
        }
      );
      var sorted = from l in list
        orderby l.R.Disk descending
        select l;
      foreach (var l in sorted) Instances.Add(l);
    }

    private static void ReadInitDeployment(bool printUndeployedInst = false) {
      ReadCsv(CsvDeploy, line => {
          var fields = line.Split(',');

          var mId = fields[2].Id();

          //可能初始状态没有分配机器
          if (mId == int.MinValue) return;

          var m = MachineKv[mId];

          var instId = fields[0].Id();
          var inst = InstanceKv[instId];

          var deployOk = m.AddInstance(inst);

          if (deployOk)
            inst.IsInitDeployed = true;
          else if (printUndeployedInst) Console.WriteLine(m.FailedReason(inst));
        }
      );
    }

    private static void WriteSubmitCsv(string csv) {
      if (!AllInstDeployed) {
        Console.WriteLine("Error: Not All Instances Are Deployed!");
        return;
      }

      using (var w = File.CreateText(csv)) {
        foreach (var m in Machines)
        foreach (var i in m.Instances)
          if (!i.IsInitDeployed)
            w.WriteLine($"inst_{i.Id},machine_{m.Id}");
      }
    }
  }
}