using System;
using System.IO;

namespace Tianchi {
  public static partial class Program {
    private static string _projectPath = "D:/tianchi/";

    // ReSharper disable once InconsistentNaming
    private static readonly StreamWriter w = File.CreateText(CsvSubmit);

    //这里使用固定的文件名，覆盖旧数据
    //submit_{DateTime.Now:yyyyMMdd_hhmmss}.csv";
    private static string CsvSubmit => $"{_projectPath}/submit.csv";

    private static void Main(string[] args) {
      if (args.Length == 1) _projectPath = args[0];

      ReadAllData();
      //RunFirstFit();

      GenDeploy($"{_projectPath}/search");

      w.Close();
      //*
      Console.WriteLine("==Judge==");
      JudgeSubmit(CsvSubmit); //*/

      Console.WriteLine("==End==");
    }
  }
}