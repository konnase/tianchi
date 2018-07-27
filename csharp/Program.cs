using System.IO;

namespace Tianchi {
  public static partial class Program {
    public const double CpuUtilH = 0.695;
    public const double CpuUtilL = 0.58;
    private static string _projectPath = "D:/tianchi/";

    private static readonly StreamWriter _w = null;

    // ReSharper disable once ConvertToAutoProperty
    private static string[] DataSet => DataSetB;
    private static string CsvDeploy => DataPath + DataSet[3];

    //这里使用固定的文件名，覆盖旧数据
    //submit_{DateTime.Now:yyyyMMdd_hhmmss}.csv";
    private static string CsvSubmit => $"{_projectPath}/submit.csv";

    private static void Main(string[] args) {
      if (args.Length == 1) _projectPath = args[0];

      ReadAllData(DataSet);

      //_w = File.CreateText(CsvSubmit);

      //RunFirstFit();

      VerifySearchResult($"{_projectPath}/search-result/search6991");
      //GenDeploy($"{_projectPath}/search");

      //Console.WriteLine("==Judge==");
      //JudgeSubmit(CsvSubmit);
      //PrintSearch();

      //Console.WriteLine("==End==");
    }
  }
}