using System;
using System.IO;

namespace Tianchi {
  public static partial class Program {
    static void ParseOut(string fileName) {
      var mCnt = 0;
      ClearMachineDeployment(); //clean state

      var f = File.OpenText(fileName);
      string line;
      while (null != (line = f.ReadLine())) {
        // ReSharper disable once StringIndexOfIsCultureSpecific.1
        var i = line.IndexOf("inst_");
        if (i < 0 || line.Contains("undeployed")) continue;

        var instList = line.Substring(i, line.Length - i - 1).CsvToInstanceList();
        var m = Machines[mCnt++];
        foreach (var inst in instList) {
          if (!m.AddInstance(inst)) {
            Console.Write(m.FailedReason(inst));
            Console.Write("\t");
            Console.Write(m);
            Console.Write("\t");
            Console.WriteLine(inst);
            Console.WriteLine(m.InstListToStr());
            Console.WriteLine(line);
            Console.WriteLine();
          }
        }
      }

      f.Close();
    }
  }
}