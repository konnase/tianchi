using System.Collections.Generic;

namespace Tianchi {
  public static class SemiFinal {
    public static readonly Dictionary<string, DataSet> DsKv = Read();

    public static DataSet A => DsKv["a"];
    public static DataSet B => DsKv["b"];
    public static DataSet C => DsKv["c"];
    public static DataSet D => DsKv["d"];
    public static DataSet E => DsKv["e"];

    private static Dictionary<string, DataSet> Read() {
      MachineType.CapDiskLarge = 2457;
      Util.IsAlpha10 = false;

      var a = DataSet.Read(DataSetId.A,
        "data/scheduling_semifinal_data_20180815/app_resources.csv",
        "data/scheduling_semifinal_data_20180815/app_interference.csv",
        "data/scheduling_semifinal_data_20180815/machine_resources.a.csv",
        "data/scheduling_semifinal_data_20180815/instance_deploy.a.csv",
        "data/scheduling_semifinal_data_20180815/job_info.a.csv"
      );

      var b = DataSet.Read(DataSetId.B, a.AppKv,
        "data/scheduling_semifinal_data_20180815/machine_resources.b.csv",
        "data/scheduling_semifinal_data_20180815/instance_deploy.b.csv",
        "data/scheduling_semifinal_data_20180815/job_info.b.csv");

      var c = DataSet.Read(DataSetId.C, a.AppKv,
        "data/scheduling_semifinal_data_20180815/machine_resources.c.csv",
        "data/scheduling_semifinal_data_20180815/instance_deploy.c.csv",
        "data/scheduling_semifinal_data_20180815/job_info.c.csv");

      var d = DataSet.Read(DataSetId.D, a.AppKv,
        "data/scheduling_semifinal_data_20180815/machine_resources.d.csv",
        "data/scheduling_semifinal_data_20180815/instance_deploy.d.csv",
        "data/scheduling_semifinal_data_20180815/job_info.d.csv");

      var e = DataSet.Read(DataSetId.E, a.AppKv,
        "data/scheduling_semifinal_data_20180815/machine_resources.e.csv",
        "data/scheduling_semifinal_data_20180815/instance_deploy.e.csv",
        "data/scheduling_semifinal_data_20180815/job_info.e.csv");

      var kv = new Dictionary<string, DataSet>(capacity: 5) {
        ["A"] = a,
        ["B"] = b,
        ["C"] = c,
        ["D"] = d,
        ["E"] = e
      };

      return kv;
    }
  }
}
