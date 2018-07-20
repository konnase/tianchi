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

    public static int[] CsvToIntArray(this string csv) {
      var fields = csv.Split(',');
      var result = new int[fields.Length];
      for (var i = 0; i < fields.Length; i++) result[i] = int.Parse(fields[i]);

      return result;
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

    public static string ToStr<T>(this List<Instance> insts, Func<Instance, T> attr) {
      var list = new List<T>(insts.Count);
      foreach (var i in insts) list.Add(attr(i));

      return list.ToMergeStr();
    }

    public static string ToStr<T>(this T[] array) {
      if (array.Length == 0) return string.Empty;
      var s = new StringBuilder();

      foreach (var p in array) {
        s.Append(p);
        s.Append(",");
      }

      return s.Length > 1 ? s.ToString(0, s.Length - 1) : string.Empty;
    }

    private static string ToMergeStr<T>(this List<T> list) {
      if (list.Count == 0) return string.Empty;

      var arr = new T[list.Count];
      list.CopyTo(arr);

      Array.Sort(arr);

      var kv = new Dictionary<T, int>(list.Count);
      foreach (var i in arr)
        if (kv.ContainsKey(i))
          kv[i]++;
        else
          kv[i] = 1;

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

    public static void Each<T>(this IEnumerable<T> iter, Action<T> action) {
      foreach (var i in iter) action(i);
    }
  }
}