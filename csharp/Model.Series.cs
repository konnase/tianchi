using System;
using System.Diagnostics;
using System.Text;

namespace Tianchi {
  public class Series {
    private readonly double[] _data;

    public Series(int length) {
      Length = length;
      _data = new double[length];
    }

    public Series(int length, double initValue) : this(length) {
      for (var i = 0; i < length; i++) _data[i] = initValue;
    }

    public int Length { get; }

    public double this[int i] => _data[i];

    public double Max {
      get {
        var v = double.MinValue;
        for (var i = 0; i < Length; i++) {
          var d = _data[i];
          if (v < d) v = d;
        }

        return v;
      }
    }

    public double Min {
      get {
        var v = double.MaxValue;
        for (var i = 0; i < Length; i++) {
          var d = _data[i];
          if (v > d) v = d;
        }

        return v;
      }
    }

    public double Avg {
      get {
        var s = 0.0;
        for (var i = 0; i < Length; i++) s += _data[i];

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

    public double Average(Func<double, double> func) {
      return Sum(func) / Length;
    }

    public double Sum(Func<double, double> func) {
      var sum = 0.0;
      for (var i = 0; i < Length; i++) sum += func(_data[i]);

      return sum;
    }

    public bool Any(Predicate<int> predicate) {
      for (var i = 0; i < Length; i++)
        if (predicate(i))
          return true;

      return false;
    }

    public void CopyFrom(Series s) {
      if (s.Length == Length) {
        for (var i = 0; i < Length; i++) _data[i] = s._data[i];
      } else if (s.Length == Resource.Ts98) {
        var sLen = s.Length;
        const int interval = Resource.Interval;
        Debug.Assert(sLen * interval == Length);

        for (var i = 0; i < sLen; i++)
        for (var j = 0; j < interval; j++)
          _data[i * interval + j] = s[i];
      } else {
        throw new Exception($"[CopyFrom] Dimension mismatch: {Length} vs {s.Length}");
      }
    }

    public Series Clone() {
      var s = new Series(Length);
      s.CopyFrom(this);
      return s;
    }

    public void Reset() {
      for (var i = 0; i < Length; i++) _data[i] = 0.0;
    }

    // 将array各项累加到Series对应项
    public void Add(Series s) {
      if (s.Length == Length) { //相同维度
        for (var i = 0; i < Length; i++) _data[i] += s._data[i]; //这里不做超限检查
      } else if (s.Length == Resource.Ts98) { //不同维度，且this(机器使用量) 1470 - 在线App 98
        var sLen = s.Length;
        const int interval = Resource.Interval;
        Debug.Assert(sLen * interval == Length);

        for (var i = 0; i < sLen; i++)
        for (var j = 0; j < interval; j++)
          _data[i * interval + j] += s[i];
      } else {
        throw new Exception($"[Add] Dimension mismatch: {Length} vs {s.Length}");
      }
    }

    public void Subtract(Series s) {
      if (s.Length == Length) { //相同维度
        for (var i = 0; i < Length; i++) _data[i] -= s._data[i]; //这里不做超限检查
      } else if (s.Length == Resource.Ts98) { //不同维度，且this(机器使用量) 1470 - 在线App 98
        var sLen = s.Length;
        const int interval = Resource.Interval;
        Debug.Assert(sLen * interval == Length);

        for (var i = 0; i < sLen; i++)
        for (var j = 0; j < interval; j++)
          _data[i * interval + j] -= s[i];
      } else {
        throw new Exception($"[Subtract] Dimension mismatch: {Length} vs {s.Length}");
      }
    }

    // 从总容量capacity中减去s，将this对应的值设置为差值
    // 这里要求 this, capacity 和 s 维度相同
    public void SubtractByCapacity(Series capacity, Series s) {
      if (s.Length != capacity.Length || s.Length != Length)
        throw new Exception($"[SubtractByCapacity] Dimension mismatch: {Length} vs {s.Length}");

      for (var i = 0; i < _data.Length; i++) _data[i] = capacity._data[i] - s._data[i]; //这里不做超限检查
    }

    // 计算 当前序列 this + s 之后 向量的最大值
    // 不会修改当前序列
    // s 的维度可以不同
    public double MaxWith(Series s) {
      var v = double.MinValue;
      if (s.Length == Length) {
        for (var i = 0; i < Length; i++) {
          var d = _data[i] + s[i];
          if (v < d) v = d;
        }
      } else if (s.Length == Resource.Ts98) {
        var sLen = s.Length;
        const int interval = Resource.Interval;
        Debug.Assert(sLen * interval == Length);

        for (var i = 0; i < sLen; i++)
        for (var j = 0; j < interval; j++) {
          var d = _data[i * interval + j] + s[i];
          if (v < d) v = d;
        }
      } else {
        throw new Exception($"[MaxWith] Dimension mismatch: {Length} vs {s.Length}");
      }

      return v;
    }

    //返回一段区间（包含首尾）内最大值对应的时刻索引
    public int TsOfSpanMax(int start, int end) {
      var max = double.MinValue;
      var ts = -1;
      for (var i = start; i <= end; i++) {
        var d = _data[i];
        if (max < d) {
          max = d;
          ts = i;
        }
      }

      return ts;
    }

    public static Series Parse(string[] fields) {
      var length = fields.Length; //对App，还保持98个点
      var s = new Series(length);
      for (var i = 0; i < length; i++) {
        var d = double.Parse(fields[i]);
        s._data[i] = d;
      }

      return s;
    }

    public override string ToString() {
      var s = new StringBuilder();

      foreach (var i in _data) s.Append($"{i:0.00},");

      return s.Length > 1 ? s.ToString(0, s.Length - 1) : string.Empty;
    }
  }
}