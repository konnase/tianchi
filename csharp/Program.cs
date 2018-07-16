using System;
using System.IO;

namespace Tianchi {
  public static partial class Program {
    private static string _projectPath = "D:/tianchi/";

    //这里使用固定的文件名，覆盖旧数据
    private static string CsvSubmit => $"{_projectPath}/submit.csv";
    //submit_{DateTime.Now:yyyyMMdd_hhmmss}.csv";

    private static StreamWriter _writer;

    private static void Main(string[] args) {
      if (args.Length == 1) {
        _projectPath = args[0];
      }

      ReadAllData();

      VerifySearchResult($"{_projectPath}/search");

      //ParseBins($"{_projectPath}/bins_wx_uniq.txt");
      //SetInstDiskKv();

      /*
      RunFf();

      Console.WriteLine("==Judge==");
      JudgeSubmit(CsvSubmit);//*/

      Console.WriteLine("==End==");
    }
  }
}