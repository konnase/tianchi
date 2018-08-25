using static System.Console;

namespace Tianchi {
  public static class Program {
    //注意：
    //所有文件路径均假设当前目录为项目的根目录
    //可以在Rider的执行参数中设置，
    //命令行则在项目根目录执行 dotnet run --project cssharp/tianchi.csproj
    private static void Main() {
      foreach (var ds in DataSetSemi.SemiDSs) {
        Write($"JobCnt: {ds.JobCount}\t");
        WriteLine(ds.InitSolution.ScoreMsg);
      }

      WriteLine("==End==");
    }
  }
}
