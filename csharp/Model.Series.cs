using System;
using System.Text;
using static Tianchi.Resource;

namespace Tianchi {
  public class Series {
    private readonly double[] _data;

    public Series(int length) {
      Length = length;
      _data = new double[length];
    }

    public Series(int length, double initValue) : this(length) {
      for (var i = 0; i < length; i++) {
        _data[i] = initValue;
      }
    }

    public int Length { get; }

    public double this[int i] => _data[i];

    public double Max {
      get {
        var v = double.MinValue;
        for (var i = 0; i < Length; i++) {
          var d = _data[i];
          if (v < d) {
            v = d;
          }
        }

        return v;
      }
    }

    public double Min {
      get {
        var v = double.MaxValue;
        for (var i = 0; i < Length; i++) {
          var d = _data[i];
          if (v > d) {
            v = d;
          }
        }

        return v;
      }
    }

    public double Avg {
      get {
        var s = 0.0;
        for (var i = 0; i < Length; i++) {
          s += _data[i];
        }

        return s / Length;
      }
    }

    public double Stdev {
      get {
        var avg = Avg;
        var sqrSum = 0.0;
        for (var i = 0; i < Length; i++) {
          var d = _data[i];
          sqrSum += (d - avg) * (d - avg);
        }

        return Math.Sqrt(sqrSum / Length);
      }
    }

    public bool Any(Predicate<int> predicate) {
      for (var i = 0; i < Length; i++) {
        if (predicate(i)) {
          return true;
        }
      }

      return false;
    }

    public Series CopyFrom(Series s) {
      if (s.Length == Length) {
        for (var i = 0; i < Length; i++) {
          _data[i] = s._data[i];
        }
      } else if (s.Length == T98) { // 兼容：同时支持98和1470个点的Series
        for (var i = 0; i < T98; i++)
        for (var j = 0; j < Interval; j++) {
          _data[i * Interval + j] = s[i];
        }
      } else {
        throw new Exception($"[CopyFrom] Dimension mismatch: {Length} vs {s.Length}");
      }

      return this;
    }

    public Series Clone() {
      var s = new Series(Length);
      s.CopyFrom(this);
      return s;
    }

    public Series Reset() {
      for (var i = 0; i < Length; i++) {
        _data[i] = 0.0;
      }

      return this;
    }

    public bool AnyLargerThan(Series s) {
      if (Length == s.Length) {
        for (var i = 0; i < Length; i++) {
          if (_data[i] > s._data[i]) { //TODO: Round
            return true;
          }
        }
      } else if (Length == T98 && s.Length == T1470) {
        for (var i = 0; i < T98; i++) {
          var n = i * Interval;
          for (var j = 0; j < Interval - 1; j++) {
            // ReSharper disable once CompareOfFloatsByEqualityOperator
            if (s._data[n + j] != s._data[n + j + 1]) {
              throw new Exception("[AnyLargerThan] Values in the same interval are not equal!");
            }
          }

          if (_data[i] > s._data[n]) {
            return true;
          }
        }
      } else {
        throw new Exception("[AnyLargerThan] Unknown dimensions!");
      }

      return false;
    }

    /// <summary>
    ///   如果this是由T98维直接扩展到T1470维的，将其压缩回T98维，
    ///   为了减少GC，将结果保存到参数 s 中
    /// </summary>
    public void ShrinkTo(Series s) {
      if (Length != T1470 || s.Length != T98) {
        throw new Exception("[ShrinkTo] Unknown dimensions!");
      }

      for (var i = 0; i < T98; i++) {
        var n = i * Interval;
        for (var j = 0; j < Interval - 1; j++) {
          // ReSharper disable once CompareOfFloatsByEqualityOperator
          if (_data[n + j] != _data[n + j + 1]) {
            throw new Exception("[ShrinkTo] Values in the same interval are not equal!");
          }
        }

        s._data[i] = _data[n];
      }
    }

    // 将 s 累加到对应项
    public Series Add(Series s) {
      if (s.Length == Length) {
        for (var i = 0; i < Length; i++) {
          _data[i] += s._data[i];
        }
      } else if (s.Length == T98) { //兼容：不同维度，且this(机器使用量) 1470 + 在线App 98
        for (var i = 0; i < T98; i++)
        for (var j = 0; j < Interval; j++) {
          _data[i * Interval + j] += s[i];
        }
      } else {
        throw new Exception($"[Add] Dimension mismatch: {Length} vs {s.Length}");
      }

      return this;
    }

    public Series Add(double value, int begin, int length) {
      var end = begin + length;

      if (end > Length) {
        throw new IndexOutOfRangeException(
          $"[Series Add]: {begin} + {length} = {end} > {Length}");
      }

      for (var i = begin; i < end; i++) {
        _data[i] += value;
      }

      return this;
    }

    public Series Subtract(double value, int begin, int length) {
      var end = begin + length;

      if (end > Length) {
        throw new IndexOutOfRangeException(
          $"[Series Add]: {begin} + {length} = {end} > {Length}");
      }

      for (var i = begin; i < end; i++) {
        _data[i] -= value;
      }

      return this;
    }

    public Series Subtract(Series s) {
      if (s.Length == Length) {
        for (var i = 0; i < Length; i++) {
          _data[i] -= s._data[i]; //这里不做超限检查
        }
      } else if (s.Length == T98) { //不同维度，且this(机器使用量) 1470 - 在线App 98
        for (var i = 0; i < T98; i++)
        for (var j = 0; j < Interval; j++) {
          _data[i * Interval + j] -= s[i];
        }
      } else {
        throw new Exception($"[Subtract] Dimension mismatch: {Length} vs {s.Length}");
      }

      return this;
    }

    // 将this对应的值设置为 a 与 b 的和
    // 注意：会覆盖 this 的旧值，但不会修改 a 或 b
    public Series SumOf(Series a, Series b) {
      if (b.Length == Length && a.Length == Length) { //相同维度
        for (var i = 0; i < Length; i++) {
          _data[i] = a._data[i] + b._data[i]; //这里不做超限检查
        }
      } else if (b.Length == T98 && a.Length == T1470 && Length == T1470) {
        for (var i = 0; i < T98; i++)
        for (var j = 0; j < Interval; j++) {
          _data[i * Interval + j] = a._data[i * Interval + j] + b[i];
        }
      } else if (b.Length == T98 && a.Length == T98 && Length == T1470) {
        for (var i = 0; i < T98; i++) {
          var s = a._data[i] + b[i];
          for (var j = 0; j < Interval; j++) {
            _data[i * Interval + j] = s;
          }
        }
      } else {
        throw new Exception("[SubtractByCapacity] Dimension mismatch: " +
                            $"this {Length} vs cap {a.Length}, s {b.Length}");
      }

      return this;
    }

    // 将this对应的值设置为 capacity 与 s 的差值
    // 注意：会覆盖 this 的旧值，但不会修改 capacity 或 s
    public Series DiffOf(Series capacity, Series s) {
      if (s.Length == Length && capacity.Length == Length) { //相同维度
        for (var i = 0; i < Length; i++) {
          _data[i] = capacity._data[i] - s._data[i]; //这里不做超限检查
        }
      } else if (s.Length == T98 && capacity.Length == T1470 && Length == T1470) {
        for (var i = 0; i < T98; i++)
        for (var j = 0; j < Interval; j++) {
          _data[i * Interval + j] = capacity._data[i * Interval + j] - s[i];
        }
      } else if (s.Length == T98 && capacity.Length == T98 && Length == T1470) {
        for (var i = 0; i < T98; i++) {
          var d = capacity._data[i] - s[i];
          for (var j = 0; j < Interval; j++) {
            _data[i * Interval + j] = d;
          }
        }
      } else {
        throw new Exception("[SubtractByCapacity] Dimension mismatch: " +
                            $"this {Length} vs cap {capacity.Length}, s {s.Length}");
      }

      return this;
    }

    // 计算 当前序列 this + s 之后 向量的最大值
    // 不会修改当前序列
    // s 的维度可以不同
    public double MaxWith(Series s) {
      var v = double.MinValue;
      if (s.Length == Length) {
        for (var i = 0; i < Length; i++) {
          var d = _data[i] + s[i];
          if (v < d) {
            v = d;
          }
        }
      } else if (s.Length == T98) {
        for (var i = 0; i < T98; i++)
        for (var j = 0; j < Interval; j++) {
          var d = _data[i * Interval + j] + s[i];
          if (v < d) {
            v = d;
          }
        }
      } else {
        throw new Exception($"[MaxWith] Dimension mismatch: {Length} vs {s.Length}");
      }

      return v;
    }

    // 返回一段区间内最大值的索引，若有多个相等的最大值，返回 *最大的索引*
    public int IndexOfMax(int begin, int length) {
      var end = begin + length;

      if (end > Length) {
        throw new IndexOutOfRangeException(
          $"[Series Add]: {begin} + {length} = {end} > {Length}");
      }

      var max = double.MinValue;
      var idx = -1;
      for (var i = begin; i < end; i++) {
        var d = _data[i];
        if (max < d) {
          max = d;
          idx = i;
        }
      }

      return idx;
    }

    public static Series Parse(string[] parts) {
      var length = parts.Length; // 兼容：对App，还保持98个点
      var s = new Series(length);
      for (var i = 0; i < length; i++) {
        var d = double.Parse(parts[i]);
        s._data[i] = d;
      }

      return s;
    }

    public override string ToString() {
      var s = new StringBuilder();

      foreach (var i in _data) {
        s.Append($"{i:0.00},");
      }

      return s.Length > 1 ? s.ToString(startIndex: 0, length: s.Length - 1) : string.Empty;
    }
  }
}
