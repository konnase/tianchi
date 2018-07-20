using System;
using System.IO;

namespace Tianchi {
  public static partial class Program {
    private static string _projectPath = "D:/tianchi/";

    private static StreamWriter _w;

    //这里使用固定的文件名，覆盖旧数据
    //submit_{DateTime.Now:yyyyMMdd_hhmmss}.csv";
    private static string CsvSubmit => $"{_projectPath}/submit.csv";

    private static void Main(string[] args) {
      if (args.Length == 1) _projectPath = args[0];

      ReadAllData();

      _w = File.CreateText(CsvSubmit);
      //RunFirstFit();

      //输出初始部署的机器资源占用情况
      //Machines.Where(m => !m.IsIdle).Each(Console.WriteLine);

      GenDeploy($"{_projectPath}/search");

      _w.Close();
      /*
      Console.WriteLine("==Judge==");
      JudgeSubmit(CsvSubmit); //*/

      Console.WriteLine("==End==");
    }
  }
}