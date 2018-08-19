using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Tianchi {
  public static class Ext {
    public static int Id(this string id) {
      if (string.IsNullOrEmpty(id)
          || !id.Contains('_')
          || !char.IsDigit(id.Last()))
        return int.MinValue;

      return int.Parse(id.Substring(id.IndexOf('_') + 1));
    }

    public static void ReadCsv(string csvFile, Action<string[]> action) {
      using (var csv = File.OpenText(csvFile)) {
        string line;
        while (null != (line = csv.ReadLine())) action(line.Split(','));
      }
    }

    public static int GetLineCount(string csv, bool withHeader = false) {
      var cnt = 0;
      using (var f = File.OpenText(csv)) {
        while (null != f.ReadLine()) cnt++;
      }

      return withHeader ? Math.Max(cnt - 1, 0) : cnt;
    }

    public static void ReadCsv(string csvFile, Action<string> action) {
      using (var csv = File.OpenText(csvFile)) {
        string line;
        while (null != (line = csv.ReadLine())) action(line);
      }
    }

    public static List<Instance> CsvToInstList(this string csv, Solution solution) {
      var fields = csv.Split(',');
      var result = new List<Instance>(fields.Length);
      result.AddRange(fields.Select(s => solution.GetInst(s.Id())));

      return result;
    }

    public static void Init<T>(this T[] array, T v) {
      for (var i = 0; i < array.Length; i++) array[i] = v;
    }

    public static void Init<T>(this T[,] array, T v) {
      for (var i = 0; i < array.GetUpperBound(0); i++)
      for (var j = 0; j < array.GetUpperBound(1); j++)
        array[i, j] = v;
    }

    public static string ToStr<TS, T>(this ICollection<TS> list, Func<TS, T> func) {
      var result = new List<T>(list.Count);
      foreach (var i in list) result.Add(func(i));

      return result.ToStr();
    }

    public static string ToStr<T>(this ICollection<T> list) {
      if (list.Count == 0) return string.Empty;
      var s = new StringBuilder();

      foreach (var p in list) {
        s.Append(p);
        s.Append(",");
      }

      return s.Length > 1 ? s.ToString(0, s.Length - 1) : string.Empty;
    }

    //private static string ToMergeStr<T>(this ICollection<T> list) {
    //  if (list.Count == 0) return string.Empty;
    //  var kv = new Dictionary<T, int>(list.Count);
    //
    //  list.OrderBy(k => k)
    //    .ForEach(k => kv[k] = kv.GetValueOrDefault(k, 0) + 1);
    //
    //  var s = new StringBuilder();
    //
    //  foreach (var p in kv) {
    //    s.Append(p.Key);
    //    if (p.Value > 1) {
    //      s.Append("*");
    //      s.Append(p.Value);
    //    }
    //
    //    s.Append(",");
    //  }
    //
    //  return s.Length > 1 ? s.ToString(0, s.Length - 1) : string.Empty;
    //}

    public static void ForEach<T>(this IEnumerable<T> iter, Action<T> action) {
      foreach (var i in iter) action(i);
    }

    public static void ForEach<T>(this IList<T> collection, Action<T, int> action) {
      for (var i = 0; i < collection.Count; i++) action(collection[i], i);
    }

    public static void Shuffle<T>(this IList<T> list, Random rnd) {
      for (var i = 0; i < list.Count; i++) {
        var j = rnd.Next(i, list.Count);
        var temp = list[i];
        list[i] = list[j];
        list[j] = temp;
      }
    }

    public static void Shuffle<T>(this IList<T> list) {
      list.Shuffle(new Random());
    }

    public static int[] ToRangeArray(this int n) {
      var result = new int[n];
      for (var i = 0; i < n; i++) result[i] = i;

      return result;
    }

    public static double Score(this Series cpuUsage, double cpuCap,
      double alpha = 10, double beta = 0.5) {
      var sum = 0.0;
      const int cnt = Resource.TsCount;
      for (var ts = 0; ts < cnt; ts++) {
        var c = cpuUsage[ts] / cpuCap;
        sum += c <= beta ? 1.0 : alpha * Math.Exp(c - beta) - alpha + 1.0;
      }

      return sum / cnt;
    }

    public static string ToStr(this DataSetId id) {
      switch (id) {
        case DataSetId.PreA: return "pre_a";
        case DataSetId.PreB: return "pre_b";
        case DataSetId.SemiA: return "semi_a";
        case DataSetId.SemiB: return "semi_b";
        case DataSetId.SemiC: return "semi_c";
        case DataSetId.SemiD: return "semi_d";
        case DataSetId.SemiE: return "semi_e";
      }

      return string.Empty;
    }
  }

  public enum DataSetId {
    PreA,
    PreB,
    SemiA,
    SemiB,
    SemiC,
    SemiD,
    SemiE
  }
}