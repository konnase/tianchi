using System.Diagnostics;
using System.IO;

namespace Tianchi {
  public static partial class Program {
    private static void VerifySearchResult(string fileName) {
      var mCnt = 0;
      ClearMachineDeployment(); //clean state

      //格式
      //total(0.500000,600): {80,100,80,100,80,80,80} (inst_6297,inst_20827,...)

      var f = File.OpenText(fileName);
      string line;
      while (null != (line = f.ReadLine())) {
        var cm = line.IndexOf(',');
        var rp = line.IndexOf(')'); //total(0.500000,600)
        var totalDisk = int.Parse(line.Substring(cm + 1, rp - cm - 1));

        // ReSharper disable once StringIndexOfIsCultureSpecific.1
        var i = line.IndexOf("inst_");
        if (i < 0) continue;

        var instList = line.Substring(i, line.Length - i - 1).CsvToInstanceList();
        var m = Machines[mCnt++];
        foreach (var inst in instList) {
          Debug.Assert(m.CapDisk >= totalDisk);
          m.AddInstance(inst, ignoreCheck: true);
        }
      }

      f.Close();

      FinalCheck(true);
    }
  }
}