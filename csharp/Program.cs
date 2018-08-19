using System;

namespace Tianchi {
  public static class Program {
    //注意：
    //所有文件路径均假设当前目录为项目的根目录
    //可以在Rider的执行参数中设置，
    //命令行则在项目根目录执行 dotnet run --project cssharp/tianchi.csproj
    private static void Main() {
      //DataSetPreA.Run("search-result/search_PreA_5592_30_5590m");
      DataSetPreB.Run("search-result/search_PreB_6636_09_4813m");

      Console.WriteLine("==End==");
    }
  }
}