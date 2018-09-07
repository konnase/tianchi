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
      Solution.ReadAppSubmit(sol, "submit_file_a_4563.csv");
      WriteLine($"[{semiA.Id}]: Score of submit_file_a: {sol.ActualScore: 0.0000}; ");
      JobDeploy.FirstFit(sol);
      WriteLine($"[{semiA.Id}]: Score of App+Job: {sol.ActualScore: 0.0000}; ");

      var csvSubmit = $"submit.a.csv"; // submit.a.csv是复制的submit_file_a_xxxx
      var writer = File.AppendText(csvSubmit);
      writer.WriteLine(); //注意格式，submit.a.csv最后没有空行，要填上
      Solution.SaveJobSubmit(sol, writer);
      writer.Close();

      Solution.CheckAllDeployed(sol);
      WriteLine("==End==");
      
      
      var semiB = DataSetSemi.SemiB;
      var solB = semiB.InitSolution.Clone();
      WriteLine($"[{semiB.Id}]: Score of App: {solB.ActualScore: 0.0000}; ");
      Solution.ReadAppSubmit(solB, "submit_file_b_4567.csv");
      WriteLine($"[{semiB.Id}]: Score of submit_file_b: {solB.ActualScore: 0.0000}; ");
      JobDeploy.FirstFit(solB);
      WriteLine($"[{semiB.Id}]: Score of App+Job: {solB.ActualScore: 0.0000}; ");

      var csvSubmitB = $"submit.b.csv";
      var writerB = File.AppendText(csvSubmitB);
      writerB.WriteLine();
      Solution.SaveJobSubmit(solB, writerB);
      writerB.Close();

      WriteLine("==End==");
      
      var semiC = DataSetSemi.SemiC;
      var solC = semiC.InitSolution.Clone();
      WriteLine($"[{semiC.Id}]: Score of App: {solC.ActualScore: 0.0000}; ");
      Solution.ReadAppSubmit(solC, "submit_file_c_7480.csv");
      WriteLine($"[{semiC.Id}]: Score of submit_file_c: {solC.ActualScore: 0.0000}; ");
      JobDeploy.FirstFit(solC);
      WriteLine($"[{semiC.Id}]: Score of App+Job: {solC.ActualScore: 0.0000}; ");

      var csvSubmitC = $"submit.c.csv";
      var writerC = File.AppendText(csvSubmitC);
      writerC.WriteLine();
      Solution.SaveJobSubmit(solC, writerC);
      writerC.Close();

      WriteLine("==End==");
      
      var semiD = DataSetSemi.SemiD;
      var solD = semiD.InitSolution.Clone();
      WriteLine($"[{semiD.Id}]: Score of App: {solD.ActualScore: 0.0000}; ");
      Solution.ReadAppSubmit(solD, "submit_file_d_7471.csv");
      WriteLine($"[{semiD.Id}]: Score of submit_file_d: {solD.ActualScore: 0.0000}; ");
      JobDeploy.FirstFit(solD);
      WriteLine($"[{semiD.Id}]: Score of App+Job: {solD.ActualScore: 0.0000}; ");

      var csvSubmitD = $"submit.d.csv";
      var writerD = File.AppendText(csvSubmitD);
      writerD.WriteLine();
      Solution.SaveJobSubmit(solD, writerD);
      writerD.Close();

      WriteLine("==End==");
      
      var semiE = DataSetSemi.SemiE;
      var solE = semiE.InitSolution.Clone();
      WriteLine($"[{semiE.Id}]: Score of App: {solE.ActualScore: 0.0000}; ");
      Solution.ReadAppSubmit(solE, "submit_file_e_8974.csv");
      WriteLine($"[{semiE.Id}]: Score of submit_file_e: {solE.ActualScore: 0.0000}; ");
      JobDeploy.FirstFit(solE);
      WriteLine($"[{semiE.Id}]: Score of App+Job: {solE.ActualScore: 0.0000}; ");

      var csvSubmitE = $"submit.e.csv";
      var writerE = File.AppendText(csvSubmitE);
      writerE.WriteLine();
      Solution.SaveJobSubmit(solE, writerE);
      writerE.Close();

      WriteLine("==End==");

      var finalScore = (sol.ActualScore + solB.ActualScore + solC.ActualScore + solD.ActualScore + solE.ActualScore) /
                       5;
      WriteLine($"[{semiE.Id}]: Score of App+Job: {finalScore: 0.0000}; ");
    }
  }
}
