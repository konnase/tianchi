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
    private App(string[] parts) {
      Id = parts[0].Id();
      R = new Resource(
        GetSeries(parts[1]),
        GetSeries(parts[2]),
        Convert.ToInt32(double.Parse(parts[3])),
        int.Parse(parts[4]),
        int.Parse(parts[5]),
        int.Parse(parts[6])
      );
    }

    public void AddXRule(int otherAppId, int k) {
      k += Id == otherAppId ? 1 : 0; //Hack: 同类应用自身的冲突限制需要特别处理
      _xRules[otherAppId] = k;
    }

    public int XLimit(int otherAppId) {
      return _xRules.GetValueOrDefault(otherAppId, int.MaxValue);
    }

    public static App Parse(string[] parts) {
      return new App(parts);
    }

    #region Hack FreqSeriesKv 

    private static readonly Dictionary<string, Series> FreqSeriesKv = InitFreqSeriesKv();

    private static Dictionary<string, Series> InitFreqSeriesKv() {
      var kv = new Dictionary<string, Series>(capacity: 7);
      const string s1 =
        "0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000|0.500000";
      const string s2 =
        "1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000|1.000000";
      const string s3 =
        "4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000|4.000000";
      const string s4 =
        "8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000|8.000000";
      const string s5 =
        "10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000|10.000000";
      const string s6 =
        "16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000|16.000000";
      const string s7 =
        "32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000|32.000000";

      kv[s1] = Series.Parse(s1.Split(separator: '|')); //App中仍保持98个数据点
      kv[s2] = Series.Parse(s2.Split(separator: '|'));
      kv[s3] = Series.Parse(s3.Split(separator: '|'));
      kv[s4] = Series.Parse(s4.Split(separator: '|'));
      kv[s5] = Series.Parse(s5.Split(separator: '|'));
      kv[s6] = Series.Parse(s6.Split(separator: '|'));
      kv[s7] = Series.Parse(s7.Split(separator: '|'));
      return kv;
    }

    private static Series GetSeries(string str) {
      if (!FreqSeriesKv.TryGetValue(str, out var s)) {
        s = Series.Parse(str.Split(separator: '|'));
      }

      return s;
    }

    #endregion
  }

  public class AppInst {
    public readonly App App;
    public readonly int Id;
    public bool IsDeployed = false;

    // Machine 字段在不同的 Solution 是不同的对象
    public Machine Machine;

    // 迁移前所在机器，仅当 TryPut 不是 autoRemove 才使用此字段
    public Machine PreMachine;

    public AppInst(int id, App app) {
      Id = id;
      App = app;
    }

    public Resource R => App.R;

    public AppInst Clone() {
      return new AppInst(Id, App);
    }

    public override string ToString() {
      return $"inst_{Id},app_{App.Id},{R}";
    }
  }
}
