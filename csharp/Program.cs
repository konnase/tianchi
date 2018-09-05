using static System.Console;
using static Tianchi.Util;

namespace Tianchi {
  public static class Program {
    //注意：
    //所有文件路径均假设当前目录为项目的根目录
    //可以在Rider的执行参数中设置，
    //命令行则在项目根目录执行 dotnet run --project cssharp/tianchi.csproj
    private static void Main(string[] args) {
      if (args.Length != 2) {
        WriteLine("Please input one of [a,b,c,d,e], \n" +
                  "and an integer for the execution time for each round.");
        return;
      }

      var ds = args[0].ToUpper();

      if (ds.Length != 1 || ds[index: 0] < 'A' || ds[index: 0] > 'E') {
        WriteLine($"Your input is {ds}; Please input one of [a,b,c,d,e]. ");
        return;
      }

      if (!int.TryParse(args[1], out var timeout)) {
        timeout = 20;
        WriteLine($"Ill formatted integer. Using default {timeout} min.");
      }

      RunTabuSearch(ds, timeout);
    }

    private static void RunTabuSearch(string ds, long timeout) {
      var dataSet = SemiFinal.DsKv[ds];
      var sol = dataSet.InitSolution.Clone();
      timeout = timeout * Min;
      var round = 1;
      var submitOut = "";

      var s = new Search(sol);
      //搜索结束后（超时或Ctrl+C），会保存所有轮次的迁移动作，并返回输出的文件名
      //注意：solution的分数需要调用CalcActualScore()手动计算，
      //除非确认没有变更，才可以使用上次的计算结果（ActualScore）
      //      
      submitOut = s.Run(round, submitOut, timeout);
      WriteLine($"[{ds}]: Score of {submitOut}@r{round}: {sol.CalcActualScore(): 0.0000}; ");
      //
      round = 2;
      submitOut = s.Run(round, submitOut, timeout);
      WriteLine($"[{ds}]: Score of {submitOut}@r{round}: {sol.CalcActualScore(): 0.0000}; ");
      //
      round = 3;
      submitOut = s.Run(round, submitOut, timeout);
      Solution.ReleaseAllPendingInsts(sol);
      WriteLine($"[{ds}]: Score of {submitOut}@r{round}: {sol.CalcActualScore(): 0.0000}; ");
      //
      var clone = dataSet.InitSolution.Clone();
      Solution.ReadAppSubmit(clone, submitOut, verbose: true);
      //注意：读入后，需手动释放未决的 inst，否则会影响总的成本分数
      Solution.ReleaseAllPendingInsts(clone);
      WriteLine($"[{ds}]: Verify Score of {submitOut}: {clone.CalcActualScore(): 0.0000}; ");
    }

    /*
    private static void SaveSubmit(string ds, string submitIn) {
      var dataSet = SemiFinal.DsKv[ds.ToLower()];
      var sol = dataSet.InitSolution.Clone();
      WriteLine($"[{ds}]: Score of App: {sol.ActualScore: 0.0000}; ");
      Solution.ReadAppSubmit(sol, submitIn);
      Solution.ReleaseAllPendingInsts(sol);
      WriteLine($"[{ds}]: Score of {submitIn}: {sol.ActualScore: 0.0000}; ");

      if (dataSet.JobKv.Count > 0) {
        JobDeploy.FirstFit(sol);
        WriteLine($"[{ds}]: Score of App+Job: {sol.ActualScore: 0.0000}; ");

        var writer = File.CreateText($"submit_{ds}_job.csv");
        Solution.SaveJobSubmit(sol, writer);
        writer.Close();
      }

      Solution.CheckAllDeployed(sol);
      WriteLine("==End==");
    }//*/
  }
}
