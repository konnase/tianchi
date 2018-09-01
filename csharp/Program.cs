using System.IO;
using static System.Console;

namespace Tianchi {
  public static class Program {
    //注意：
    //所有文件路径均假设当前目录为项目的根目录
    //可以在Rider的执行参数中设置，
    //命令行则在项目根目录执行 dotnet run --project cssharp/tianchi.csproj
    private static void Main() {
      var semiA = DataSetSemi.SemiA;
      var sol = semiA.InitSolution.Clone();
      WriteLine($"[{semiA.Id}]: Score of App: {sol.ActualScore: 0.0000}; ");
      Solution.ReadAppSubmit(sol, "submit_file_a_4846.csv");
      WriteLine($"[{semiA.Id}]: Score of submit_file_a_4846: {sol.ActualScore: 0.0000}; ");
      JobDeploy.FirstFit(sol);
      WriteLine($"[{semiA.Id}]: Score of App+Job: {sol.ActualScore: 0.0000}; ");

      var csvSubmit = $"submit.a.csv"; // submit.a.csv是复制的submit_file_a_xxxx
      var writer = File.AppendText(csvSubmit);
      writer.WriteLine(); //注意格式，submit.a.csv最后没有空行，要填上
      Solution.SaveJobSubmit(sol, writer);
      writer.Close();

      Solution.CheckAllDeployed(sol);
      WriteLine("==End==");
    }
  }
}
