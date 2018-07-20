using System;
using System.Linq;

namespace Tianchi {
  public static partial class Program {
    private static void PrintScore() {
      Console.WriteLine($"TotalScore: " +
                        $"{TotalCostScore:0.00} / {UsedMachineCount}" +
                        $" = [{TotalCostScore / UsedMachineCount:0.00}]");
    }

    private static void PrintUndeployedInst() {
      foreach (var inst in Instances)
        if (inst.NeedDeployOrMigrate)
          Console.WriteLine(inst);
    }

    private static void PrintCsvInitInfo() {
      Console.WriteLine("==Init==");
      Console.WriteLine($"App: {AppKv.Count}, " +
                        $"Instance: {Instances.Length}, " +
                        $"Machine: {Machines.Count}");

      //FinalCheck(true); //151;
      var instList = (from i in Instances
        where i.DeployedMachine != null && i.NeedDeployOrMigrate
        select i).ToList(); //初始被忽略约束，强制部署到机器上的实例
      Console.WriteLine(instList.Count()); //143; 没有资源超限的初始部署实例；

      //注意：与上述FinalCheck结果不同，但这是合理的；
      //一台机器上可能有多个实例发生冲突，导致计数出入。
      //共117台机器上存在约束冲突
      foreach (var inst in instList)
        //inst可能存在多个冲突，这里不再一一找出，只输出机器上的应用计数列表
        Console.WriteLine($"m_{inst.DeployedMachine.Id},app_{inst.App.Id}," +
                          $"inst_{inst.Id},{inst.DeployedMachine.AppListToStr()}");
    }
  }
}