using static System.Console;

namespace Tianchi {
  public static class Program {
    //注意：
    //所有文件路径均假设当前目录为项目的根目录
    //可以在Rider的执行参数中设置，
    //命令行则在项目根目录执行 dotnet run --project cssharp/tianchi.csproj
    private static void Main() {
      var sol = DataSetSemi.SemiA.InitSolution.Clone();
      //Write($"[{ds.Id}]: Score of App: {sol.ActualScore: 0.0000}; ");
      //NaiveSearch.Run(sol, "search-result/search_SemiA_16009_32_2780m", 120 * Min);
      NaiveSearch.ReadResult(sol, "search-result/search_SemiA_16009_32_2780m");
      var clone = DataSetSemi.SemiA.InitSolution.Clone();
      if (!Solution.TrySaveAppSubmit(sol, clone)) {
        WriteLine(";-(");
      }
      //JobDeploy.FirstFit(sol);
      //WriteLine($" App+Job: {sol.ActualScore: 0.0000}; ");
      //WriteLine($"AllJobDeployed? {sol.AllJobDeployed}; Score: {sol.ActualScore: 0.0000}");
      //Solution.SaveAndJudge(sol);

      //sol = DataSetSemi.SemiB.InitSolution.Clone();
      //NaiveSearch.Run(sol,"search-result/search_SemiB_17785_61_2772m");

      //WriteLine($"Final Score: {score / cnt:0.0000}");
      WriteLine("==End==");
    }
  }
}
