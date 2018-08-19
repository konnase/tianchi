using System;
using System.Collections.Generic;
using System.Linq;

namespace Tianchi {
  //DataSet存储数据集只读的部分
  //但也有一份默认的机器和实例列表
  //并发执行多个部署方案时（如参数调优），可克隆一份机器和实例列表
  public class DataSet {
    public int AppCount;
    public int InstCount;
    public int MachineCount;
    public DataSetId Id { get; private set; }

    //instCsv里有实例Id和部署信息
    public string InstCsv { get; private set; }

    //App是只读的，每个数据集只需保存一份
    public Dictionary<int, App> AppKv { get; private set; }

    //默认的部署方案
    public Solution DefaultSolution { get; private set; }

    public Instance[] Instances => DefaultSolution.Instances;
    public Machine[] Machines => DefaultSolution.Machines;

    public static DataSet Read(DataSetId dataSetId, string appCsv, string machineCsv, string instCsv, string xCsv) {
      var dataSet = new DataSet {
        Id = dataSetId,
        AppCount = Ext.GetLineCount(appCsv),
        MachineCount = Ext.GetLineCount(machineCsv),
        InstCount = Ext.GetLineCount(instCsv),
        InstCsv = instCsv
      };

      dataSet.AppKv = new Dictionary<int, App>(dataSet.AppCount);
      dataSet.DefaultSolution = new Solution(dataSet);

      //注意读取顺序
      dataSet.ReadApp(appCsv);
      dataSet.ReadInterference(xCsv);
      dataSet.DefaultSolution.Read(machineCsv, instCsv);

      return dataSet;
    }

    private void ReadApp(string csv) {
      Ext.ReadCsv(csv,
        fields => {
          var appId = fields[0].Id();
          AppKv.Add(appId, App.Parse(fields));
        });
    }

    private void ReadInterference(string csv) {
      Ext.ReadCsv(csv, fields => {
        var app = AppKv[fields[0].Id()];
        var otherAppId = fields[1].Id();
        var k = int.Parse(fields[2]);

        app.AddXRule(otherAppId, k);
      });
    }

    public void PrintInitStats() {
      PrintCsvInitInfo();
      Console.WriteLine();
      PrintRequestUtil();
      //Console.WriteLine();
      //PrintAppUtilStat();
      //Console.WriteLine();
      //Machine.PrintList(Solution.Machines.Where(m => !m.IsIdle));
      //Console.WriteLine();
      //PrintUtilTs();
    }

    public void PrintCsvInitInfo() {
      Console.WriteLine("==Init==");
      Console.WriteLine($"App#: {AppCount}, " +
                        $"Instance#: {InstCount}, " +
                        $"Machine#: {MachineCount}");

      //以下输出初始数据中违反约束的情况
      DefaultSolution.FinalCheck(true);

      //初始数据中，忽略了约束，被强制部署到机器上的实例
      var xInstList = DefaultSolution.Instances
        .Where(i => i.Machine != null && !i.Deployed)
        .ToList();

      if (xInstList.Count > 0)
        Console.WriteLine($"xInstList.Count: {xInstList.Count}"); //DataSetA: 143

      var xMachineList = DefaultSolution.Machines
        .Where(m => !m.IsIdle && m.InstSet.Any(i => !i.Deployed))
        .ToList();

      if (xMachineList.Count > 0) Console.WriteLine($"xMachineList.Count: {xMachineList.Count}");

      //一台机器上可能有多个实例发生冲突，导致计数出入。
      //DataSet Pre A 共117台机器存在约束冲突
      foreach (var m in xMachineList)
      foreach (var x in m.ConflictList)
        Console.WriteLine($"m_{m.Id},A:app_{x.Item1.Id},B:app_{x.Item2.Id}," +
                          $"BCnt:{x.Item3} > BLimit:{x.Item4}");
    }

    //各时刻所有实例请求Cpu, Mem占总资源量的比例
    public void PrintRequestUtil() {
      var total = new Resource();
      DefaultSolution.Machines.ForEach(m => total.Add(m.Capacity));
      var totalCpu = total.Cpu[0];
      var totalMem = total.Mem[0];

      var req = new Resource();
      DefaultSolution.Instances.ForEach(inst => req.Add(inst.R));

      Console.WriteLine($"Disk: {req.Disk},{req.Disk * 100.0 / total.Disk:0.00}%");
      Console.WriteLine($"P: {req.P},{req.P * 100.0 / total.P:0.00}%");
      Console.WriteLine($"M: {req.M},{req.M * 100.0 / total.M:0.00}%");
      Console.WriteLine($"PM: {req.Pm},{req.Pm * 100.0 / total.Pm:0.00}%");

      Console.WriteLine();
      Console.WriteLine("ts,cpu,mem,util_cpu,util_mem");

      for (var i = 0; i < Resource.TsCount; i++)
        Console.WriteLine($"{i + 1},{req.Cpu[i]:0.0},{req.Mem[i]:0.0}," +
                          $"{req.Cpu[i] * 100 / totalCpu:0.000}%," +
                          $"{req.Mem[i] * 100 / totalMem:0.000}%");
    }

    //public void PrintAppUtilStat() {
    //  Console.WriteLine("aid,inst_cnt," +
    //                    "cpu_max,cpu_avg,cpu_min,cpu_stdev," +
    //                    "mem_max,mem_avg,mem_min,mem_stedv," +
    //                    "disk,p,m,pm");
    //  foreach (var a in AppKv.Values.OrderBy(a => a.Id)) {
    //    var r = a.R;
    //    Console.WriteLine($"app_{a.Id},{a.InstCount}," +
    //                      $"{r.Cpu.Max:0.0},{r.Cpu.Avg:0.0},{r.Cpu.Min:0.0},{r.Cpu.Stdev:0.0}," +
    //                      $"{r.Mem.Max:0.0},{r.Mem.Avg:0.0},{r.Mem.Min:0.0},{r.Mem.Stdev:0.0}," +
    //                      $"{r.Disk},{r.P},{r.M},{r.Pm}");
    //  }
    //}
    //
    //初赛 DataSet B Only
    //注意：这里cpu输出合计最大时刻ts=45的值，mem则是平均值
    //public void PrintInstRequest() {
    //  Console.WriteLine(InstCount);
    //  foreach (var inst in DefaultSolution.Instances)
    //    Console.WriteLine($"{Math.Ceiling(inst.R.Cpu[45])} " +
    //                      $"{Math.Ceiling(inst.R.Mem.Avg)} " +
    //                      $"{inst.R.Disk} " +
    //                      $"inst_{inst.Id}");
    //}
    //
    //将CPU和Mem分时数据转置一下
    //private void PrintUtilTs() {
    //  Console.WriteLine($"aid,ts,cpu,mem");
    //  foreach (var a in AppKv.Values.OrderBy(a => a.Id))
    //    for (var i = 0; i < Resource.TsCount; i++)
    //      Console.WriteLine($"{a.Id},{i + 1},{a.R.Cpu[i]},{a.R.Mem[i]}");
    //}
  }
}