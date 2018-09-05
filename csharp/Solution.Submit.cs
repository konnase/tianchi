using System.Collections.Generic;
using System.IO;
using static System.Console;

namespace Tianchi {
  public partial class Solution {
    public static int ReadAppSubmit(Solution solution, string submitIn,
      List<SubmitEntry> submits = null, bool verbose = false) {
      var failedCntResource = 0;
      var failedCntX = 0;
      var lineNo = 0;
      var prevRound = int.MinValue;

      var pendingSet = new HashSet<AppInst>(capacity: 5000);
      var curRoundInstSet = new HashSet<AppInst>(capacity: 5000);

      Util.ReadCsv(submitIn, parts => {
        if (parts.Length == 4) { // 在线有3部分，离线有4部分
          return false;
        }

        if (failedCntResource + failedCntX > 0 && !verbose) {
          return false;
        }

        var round = int.Parse(parts[0]);

        if (prevRound != round) {
          prevRound = round;
          pendingSet.ForEach(i => i.PrevMachine?.Remove(i, isFromPrevMachine: true));

          pendingSet.Clear();
          curRoundInstSet.Clear();
        }

        lineNo++;

        var inst = solution.AppInstKv[parts[1].Id()];
        var m = solution.MachineKv[parts[2].Id()];

        if (curRoundInstSet.Contains(inst)) {
          WriteLine($"[ReadAppSubmitByRound]: L{lineNo}@r{round} " +
                    "inst is deployed multiple times in the same round." +
                    $"\t{inst}\t{m}");
          return false;
        }

        curRoundInstSet.Add(inst);

        submits?.Add(new SubmitEntry(round, inst.Id, m.Id));

        if (m.TryPut(inst, autoRemove: false)) {
          pendingSet.Add(inst);
        } else {
          if (m.IsOverCapacityWith(inst)) {
            failedCntResource++;
          }

          if (m.IsConflictWith(inst)) {
            failedCntX++;
          }

          Write($"[ReadAppSubmitByRound]: L{lineNo}@r{round} ");
          Write(m.FailureMsg(inst));
          WriteLine($"\t{inst}\t{m}");
        }

        return true;
      });
      // 不释放最后一轮的 pendingInstKv 
      return prevRound;
    }

    public static void ReleaseAllPendingInsts(Solution solution) {
      solution.AppInsts.ForEach(inst =>
        inst.PrevMachine?.Remove(inst, isFromPrevMachine: true));
    }

    public static void SaveAppSubmit(List<SubmitEntry> submits, string csvSubmit) {
      var writer = File.CreateText(csvSubmit);
      submits.ForEach(entry =>
        writer.WriteLine($"{entry.Round}," +
                         $"inst_{entry.InstId}," +
                         $"machine_{entry.MachineIdDest}"));
      writer.Close();
    }
  }
}
