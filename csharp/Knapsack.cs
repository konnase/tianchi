using System;
using System.IO;

namespace Tianchi {
  public static partial class Program {
    static void ParseOut(string fileName) {
      var mCnt = 0;
      ClearMachineDeployment();//clean state

      using (var f = File.OpenText(fileName)) {
        var line = f.ReadLine();
        while (null != (line = f.ReadLine())) {
          // ReSharper disable once StringIndexOfIsCultureSpecific.1
          var i = line.IndexOf("inst_");
          if (i < 0) continue;
          var instList = line.Substring(i, line.Length - i - 1).CsvToInstanceList();
          var m = Machines[mCnt++];
          foreach (var inst in instList) {
            if (!m.AddInstance(inst)) {
              Console.Write(m.FailedReason(inst));
              Console.Write("\t");
              Console.Write(m);
              Console.Write("\t");
              Console.WriteLine(inst);
            }
          }
        }
      }
    }
  }
}