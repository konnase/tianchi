using System;
using System.IO;

namespace Tianchi {
  public partial class Program {
    private static void VerifySearchCmd(string[] args) {
      var searchFile = string.Empty;
      if (args.Length == 1) {
        _projectPath = ".";

        searchFile = $"{_projectPath}/search-result/{args[0]}";
      } else if (args.Length == 2) {
        _projectPath = args[0];
        searchFile = $"{_projectPath}/search-result/{args[1]}";
      } else {
        //需要手动把生成的dll拷贝到judge目录，并改名字……
        Console.WriteLine("Usage:\n" +
                          "dotent judge/verifysearch.dll <search file in [search-result]>\n" +
                          "or\n" +
                          "dotent judge/verifysearch.dll <project path> <search file in [search-result]>\n" +
                          "Example:\n" +
                          "dotnet judge/verifysearch.dll .. search\n" +
                          "dotnet judge/verifysearch.dll search");
        Environment.Exit(-1);
      }

      if (!File.Exists(searchFile)) {
        Console.Error.WriteLine($"Error: Cannot find search file {searchFile}");
        Environment.Exit(-1);
      }

      ReadAllData(DataSet);

      VerifySearchResult(searchFile);
    }
  }
}