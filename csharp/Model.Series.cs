using System;
using System.Text;

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
      } else if (s.Length == Resource.T98) { // 兼容：同时支持98和1470个点的Series
        var sLen = s.Length;
        const int interval = Resource.Interval;

        for (var i = 0; i < sLen; i++)
        for (var j = 0; j < interval; j++) {
          _data[i * interval + j] = s[i];
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

    // 将 s 累加到对应项
    public Series Add(Series s) {
      if (s.Length == Length) {
        for (var i = 0; i < Length; i++) {
          _data[i] += s._data[i];
        }
      } else if (s.Length == Resource.T98) { //兼容：不同维度，且this(机器使用量) 1470 + 在线App 98
        var sLen = s.Length;
        const int interval = Resource.Interval;

        for (var i = 0; i < sLen; i++)
        for (var j = 0; j < interval; j++) {
          _data[i * interval + j] += s[i];
        }
      } else {
        throw new Exception($"[Add] Dimension mismatch: {Length} vs {s.Length}");
      }

      return this;
    }

    public Series Add(double value, int start, int length) {
      var end = start + length;

      if (end > Length) {
        throw new IndexOutOfRangeException(
          $"[Series Add]: {start} + {length} = {end} > {Length}");
      }

      for (var i = start; i < end; i++) {
        _data[i] += value;
      }

      return this;
    }

    public Series Subtract(double value, int start, int length) {
      var end = start + length;

      if (end > Length) {
        throw new IndexOutOfRangeException(
          $"[Series Add]: {start} + {length} = {end} > {Length}");
      }

      for (var i = start; i < end; i++) {
        _data[i] -= value;
      }

      return this;
    }

    public Series Subtract(Series s) {
      if (s.Length == Length) {
        for (var i = 0; i < Length; i++) {
          _data[i] -= s._data[i]; //这里不做超限检查
        }
      } else if (s.Length == Resource.T98) { //不同维度，且this(机器使用量) 1470 - 在线App 98
        var sLen = s.Length;
        const int interval = Resource.Interval;

        for (var i = 0; i < sLen; i++)
        for (var j = 0; j < interval; j++) {
          _data[i * interval + j] -= s[i];
        }
      } else {
        throw new Exception($"[Subtract] Dimension mismatch: {Length} vs {s.Length}");
      }

      return this;
    }

    // 将this对应的值设置总容量capacity中减去s的差值
    // 注意：会覆盖 this 的旧值
    public Series DiffOf(Series capacity, Series s) {
      if (s.Length == Length && capacity.Length == Length) { //相同维度
        for (var i = 0; i < Length; i++) {
          _data[i] = capacity._data[i] - s._data[i]; //这里不做超限检查
        }
      } else if (s.Length == Resource.T98 && capacity.Length == Length) {
        var sLen = s.Length;
        const int interval = Resource.Interval;

        for (var i = 0; i < sLen; i++)
        for (var j = 0; j < interval; j++) {
          _data[i * interval + j] = capacity._data[i * interval + j] - s[i];
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
      } else if (s.Length == Resource.T98) {
        var sLen = s.Length;
        const int interval = Resource.Interval;

        for (var i = 0; i < sLen; i++)
        for (var j = 0; j < interval; j++) {
          var d = _data[i * interval + j] + s[i];
          if (v < d) {
            v = d;
          }
        }
      } else {
        throw new Exception($"[MaxWith] Dimension mismatch: {Length} vs {s.Length}");
      }

      return v;
    }

    // 返回一段区间内最大值的索引
    public int IndexOfMax(int start, int length) {
      var end = start + length;

      if (end > Length) {
        throw new IndexOutOfRangeException(
          $"[Series Add]: {start} + {length} = {end} > {Length}");
      }

      var max = double.MinValue;
      var idx = -1;
      for (var i = start; i < end; i++) {
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

      return s.Length > 1 ? s.ToString(0, s.Length - 1) : string.Empty;
    }
  }
}
