using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tianchi {
  public static class Util {
    public static int Id(this string id) {
      if (string.IsNullOrEmpty(id)
          || !id.Contains('_')
          || !char.IsDigit(id.Last()))
        return int.MinValue;

      return int.Parse(id.Substring(id.IndexOf('_') + 1));
    }

    public static List<Instance> CsvToInstList(this string csv) {
      var fields = csv.Split(',');
      var result = new List<Instance>(fields.Length);
      result.AddRange(fields.Select(s => Program.InstanceKv[s.Id()]));

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

    public static string ToStr<TS, T>(this IList<TS> list, Func<TS, T> func) {
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

    private static string ToMergeStr<T>(this ICollection<T> list) {
      if (list.Count == 0) return string.Empty;
      var kv = new Dictionary<T, int>(list.Count);

      list.OrderBy(k => k)
        .ForEach(k => kv[k] = kv.GetValueOrDefault(k, 0) + 1);

      var s = new StringBuilder();

      foreach (var p in kv) {
        s.Append(p.Key);
        if (p.Value > 1) {
          s.Append("*");
          s.Append(p.Value);
        }

        s.Append(",");
      }

      return s.Length > 1 ? s.ToString(0, s.Length - 1) : string.Empty;
    }

    public static void ForEach<T>(this IEnumerable<T> iter, Action<T> action) {
      foreach (var i in iter) action(i);
    }

    public static void ForEach<T>(this IList<T> collection, Action<T, int> action) {
      for (var i = 0; i < collection.Count; i++) action(collection[i], i);
    }
  }
}