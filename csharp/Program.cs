using static System.Console;

namespace Tianchi {
  public static class Program {
    //注意：
    //所有文件路径均假设当前目录为项目的根目录
    //可以在Rider的执行参数中设置，
    //命令行则在项目根目录执行 dotnet run --project cssharp/tianchi.csproj
    private static void Main() {
      foreach (var ds in DataSetSemi.SemiDSs) {
        var sol = ds.InitSolution.Clone();
        Write($"[{ds.Id}]: Score of App: {sol.ActualScore: 0.0000}; ");
        if (sol.JobTaskCount == 0) {
          WriteLine();
          continue;
        }

        JobDeploy.FirstFit(sol);
        WriteLine($"AllJobDeployed? {sol.AllJobDeployed}; Score: {sol.ActualScore: 0.0000}");

        Solution.SaveAndJudge(sol);
      }

      WriteLine("==End==");
    }
  }
}
