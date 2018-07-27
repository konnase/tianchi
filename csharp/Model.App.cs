using System;
using System.Collections.Generic;

namespace Tianchi {
  public class App {
    // 大部分<aid, bid>只有一项记录，仅少数app有几百项，
    // 这里使用 SortedList，对性能影响不大
    private readonly SortedList<int, int> _xRules = new SortedList<int, int>();
    public readonly int Id;

    public readonly Resource R;

    // 实例个数，仅用于统计分析
    public int InstanceCount;

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
      _xRules.Add(otherAppId, k);
    }

    public int XLimit(App otherApp) {
      if (_xRules.Count == 0
          || !_xRules.ContainsKey(otherApp.Id))
        return int.MaxValue;

      return _xRules[otherApp.Id];
    }

    public static App Parse(string[] fields) {
      return new App(fields);
    }
  }
}