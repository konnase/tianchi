using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tianchi {
  public static class Util {
    public static int Id(this string id) {
      if (string.IsNullOrEmpty(id)
          || !id.Contains('_')
          || !char.IsDigit(id.Last())) {
        return int.MinValue;
      }

      return int.Parse(id.Substring(id.IndexOf('_') + 1));
    }

    public static int[] CsvToIntArray(this string csv) {
      var fields = csv.Split(',');
      var result = new int[fields.Length];
      for (var i = 0; i < fields.Length; i++) result[i] = int.Parse(fields[i]);

      return result;
    }

    public static List<Instance> CsvToInstanceList(this string csv) {
      var fields = csv.Split(',');
      var result = new List<Instance>(fields.Length);
      for (var i = 0; i < fields.Length; i++) {
        var id = fields[i].Id();
        result.Add(Program.InstanceKv[id]);
      }

      return result;
    }

    public static void Init<T>(this T[] list, T v) {
      for (var i = 0; i < list.Length; i++) list[i] = v;
    }

    public static void Init<T>(this T[,] array, T v) {
      for (var i = 0; i < array.GetUpperBound(0); i++)
      for (var j = 0; j < array.GetUpperBound(1); j++)
        array[i, j] = v;
    }

    public static string ToStr<T>(this List<Instance> insts, Func<Instance, T> attr) {
      var disks = new List<T>(insts.Count);
      foreach (var i in insts) disks.Add(attr(i));

      return disks.ToMergeString();
    }

    private static string ToMergeString<T>(this List<T> list) {
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

      return s[s.Length - 1] == ','
        ? s.ToString(0, s.Length - 1)
        : s.ToString();
    }
  }
}