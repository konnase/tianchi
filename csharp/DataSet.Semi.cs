using System;

namespace Tianchi {
  public static class DataSetSemi {
    public static readonly DataSet[] SemiDSs = Read();
    public static DataSet SemiA => SemiDSs[0];
    public static DataSet SemiB => SemiDSs[1];
    public static DataSet SemiC => SemiDSs[2];
    public static DataSet SemiD => SemiDSs[3];
    public static DataSet SemiE => SemiDSs[4];

    private static DataSet[] Read() {
      MachineType.CapDiskLarge = 2457;

      var semiA = DataSet.Read(DataSetId.SemiA,
        "data/app_resources.csv",
        "data/app_interference.csv",
        "data/machine_resources.a.csv",
        "data/instance_deploy.a.csv",
        "data/job_info.a.csv"
      );

      var semiB = DataSet.Read(DataSetId.SemiB, semiA.AppKv,
        "data/machine_resources.b.csv",
        "data/instance_deploy.b.csv",
        "data/job_info.b.csv");

      var semiC = DataSet.Read(DataSetId.SemiC, semiA.AppKv,
        "data/machine_resources.c.csv",
        "data/instance_deploy.c.csv",
        "data/job_info.c.csv");

      var semiD = DataSet.Read(DataSetId.SemiD, semiA.AppKv,
        "data/machine_resources.d.csv",
        "data/instance_deploy.d.csv",
        "data/job_info.d.csv");

      var semiE = DataSet.Read(DataSetId.SemiE, semiA.AppKv,
        "data/machine_resources.e.csv",
        "data/instance_deploy.e.csv",
        "data/job_info.e.csv");

      return new[] {semiA, semiB, semiC, semiD, semiE};
    }

    public static void Run(string searchFile = "") {
      var sum = 0.0;
      foreach (var ds in SemiDSs) {
        var s = ds.InitSolution.TotalScore;
        sum += s;
        Console.WriteLine($"{s:0.0000}");
      }

      Console.WriteLine($"=====\nAvg = {sum / 5:0.0000}");
    }

    //搜索结果为：
    //使用的机器数量随cpuUtilLimit单调递减，之后基本稳定
    //但成本先下降，在0.6左右取得最小值，之后缓慢增长
    //public static void ScanCpuUtilLimit() {
    //  var tasks = new List<Task>();

    //  for (var l = 0.56; l < 0.64; l += 0.01) {
    //    var utilL = l;
    //    var t = Task.Run(() => 1);
    //    tasks.Add(t);
    //  }

    //  foreach (var t in tasks) t.Wait();
    //}
  }
}
