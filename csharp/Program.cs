using System;
using System.IO;

namespace Tianchi {
  public static partial class Program {
    public const double UtilCpuH = 1.0;
    public const double UtilCpuL = 1.0;
    private static string _projectPath = ".";

    private static readonly StreamWriter _w = null;

    // ReSharper disable once ConvertToAutoProperty
    private static string[] DataSet => DataSetB;
    private static string CsvDeploy => DataPath + DataSet[3];

    //这里使用固定的文件名，覆盖旧数据
    //submit_{DateTime.Now:yyyyMMdd_hhmmss}.csv";
    private static string CsvSubmit => $"{_projectPath}/submit.csv";

    private static void Main(string[] args) {
      //VerifySearchCmd(args);
      //RunFirstFit();

      RunSearch(args);

      Console.WriteLine("==End==");
    }
  }
}