using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using static System.Math;

namespace Tianchi {
  public static class CommonExtension {
    public static void Init<T>(this T[] array, T v) {
      for (var i = 0; i < array.Length; i++) {
        array[i] = v;
      }
    }

    public static void Init<T>(this T[,] array, T v) {
      for (var i = 0; i < array.GetUpperBound(dimension: 0); i++)
      for (var j = 0; j < array.GetUpperBound(dimension: 1); j++) {
        array[i, j] = v;
      }
    }

    public static string ToStr<TElement, TResult>(this ICollection<TElement> list,
      Func<TElement, TResult> func) {
      var result = new List<TResult>(list.Count);
      foreach (var i in list) {
        result.Add(func(i));
      }

      return result.ToStr();
    }

    public static string ToStr<T>(this ICollection<T> list) {
      if (list.Count == 0) {
        return string.Empty;
      }

      var s = new StringBuilder();

      foreach (var p in list) {
        s.Append(p);
        s.Append(",");
      }

      return s.Length > 1 ? s.ToString(startIndex: 0, length: s.Length - 1) : string.Empty;
    }

    public static void ForEach<T>(this IEnumerable<T> enumerable, Action<T> action) {
      if (enumerable == null) {
        return;
      }

      foreach (var i in enumerable) {
        action(i);
      }
    }

    public static void ForEach<T>(this IList<T> collection, Action<T, int> action) {
      if (collection == null) {
        return;
      }

      for (var i = 0; i < collection.Count; i++) {
        action(collection[i], i);
      }
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
      list.Shuffle(new Random(Seed: 1)); // Fixed seed
    }

    public static int[] ToRangeArray(this int n) {
      var result = new int[n];
      for (var i = 0; i < n; i++) {
        result[i] = i;
      }

      return result;
    }
  }

  public static class UtilExtension {
    public static int Id(this string id) {
      if (string.IsNullOrEmpty(id)
          || !id.Contains(value: '_')
          || !char.IsDigit(id.Last())) {
        return int.MinValue;
      }

      return int.Parse(id.Substring(id.IndexOf(value: '_') + 1));
    }

    public static Tuple<int, int> IdPair(this string id) {
      var ids = id.Split(separator: '-');
      var jobId = int.Parse(ids[0]);
      var taskId = int.Parse(ids[1]);
      return new Tuple<int, int>(jobId, taskId);
    }

    public static List<AppInst> CsvToAppInstList(this string csv, Solution solution) {
      var parts = csv.Split(separator: ',');
      var result = new List<AppInst>(parts.Length);
      result.AddRange(parts.Select(s => solution.AppInstKv[s.Id()]));

      return result;
    }

    public static double Score(this Series cpuUsage, double cpuCap,
      int appInstCnt) {
      //
      var alpha = Util.IsAlpha10 ? 10 : 1 + appInstCnt; // 兼容：复赛修改了评分公式
      const double beta = 0.5;

      var sum = 0.0;
      var cnt = cpuUsage.Length;
      for (var t = 0; t < cnt; t++) {
        var c = cpuUsage[t] / cpuCap;
        sum += 1.0 + alpha * (Exp(Max(c - beta, val2: 0.0)) - 1.0);
      }

      return sum / cnt;
    }
  }

  public static class Util {
    public const long Min = 60 * 1000;
    public const long Hour = 60 * Min;
    public static bool IsAlpha10 = false;

    /// <summary>
    ///   如果func返回false，则提前终止循环，
    /// </summary>
    public static void ReadCsv(string csvFile, Func<string[], bool> func) {
      using (var csv = File.OpenText(csvFile)) {
        string line;
        while (null != (line = csv.ReadLine())) {
          if (!func(line.Split(separator: ','))) {
            return;
          }
        }
      }
    }

    public static void ReadCsv(string csvFile, Action<string[]> action) {
      using (var csv = File.OpenText(csvFile)) {
        string line;
        while (null != (line = csv.ReadLine())) {
          action(line.Split(separator: ','));
        }
      }
    }

    public static void ReadCsv(string csvFile, Action<string> action) {
      using (var csv = File.OpenText(csvFile)) {
        string line;
        while (null != (line = csv.ReadLine())) {
          action(line);
        }
      }
    }

    public static int GetLineCount(string csv, bool withHeader = false) {
      var cnt = 0;
      using (var f = File.OpenText(csv)) {
        while (null != f.ReadLine()) {
          cnt++;
        }
      }

      return withHeader ? Max(cnt - 1, val2: 0) : cnt;
    }
  }
}
