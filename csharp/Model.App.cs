using System;
using System.Collections.Generic;

namespace Tianchi {
  public class App {
    // 大部分<aid, bid>只有一项记录，仅少数app有几百项，
    private readonly Dictionary<int, int> _xRules = new Dictionary<int, int>();
    public readonly int Id;

    public readonly Resource R;

    // 实例个数，仅用于统计分析
    public int InstCount;

    // ReSharper disable once SuggestBaseTypeForParameter
    private App(string[] fields) {
      Id = fields[0].Id();
      R = new Resource(
        Series.Parse(fields[1].Split('|')),
        Series.Parse(fields[2].Split('|')),
        Convert.ToInt32(double.Parse(fields[3])),
        int.Parse(fields[4]),
        int.Parse(fields[5]),
        int.Parse(fields[6])
      );
    }

    public void AddXRule(int otherAppId, int k) {
      k += Id == otherAppId ? 1 : 0; //Hack: 同类应用自身的冲突限制需要特别处理
      _xRules[otherAppId] = k;
    }

    public int XLimit(int otherAppId) {
      return !_xRules.ContainsKey(otherAppId) ? int.MaxValue : _xRules[otherAppId];
    }

    public static App Parse(string[] fields) {
      return new App(fields);
    }
  }
}