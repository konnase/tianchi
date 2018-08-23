using System;
using System.Text;

namespace Tianchi {
  public class Series {
    private readonly double[] _data;
    private readonly int _length;

    public Series(int length) {
      _data = new double[length];
      _length = length;
    }

    public Series(int length, double initValue) : this(length) {
      for (var i = 0; i < length; i++) _data[i] = initValue;
    }

    public double this[int i] => _data[i];

    public double Max {
      get {
        var v = double.MinValue;
        for (var i = 0; i < _length; i++) {
          var d = _data[i];
          if (v < d) v = d;
        }

        return v;
      }
    }

    public double Min {
      get {
        var v = double.MaxValue;
        for (var i = 0; i < _length; i++) {
          var d = _data[i];
          if (v > d) v = d;
        }

        return v;
      }
    }

    public double Avg {
      get {
        var s = 0.0;
        for (var i = 0; i < _length; i++) s += _data[i];

        return s / _length;
      }
    }

    public double Stdev {
      get {
        var avg = Avg;
        var sqrSum = 0.0;
        for (var i = 0; i < _length; i++) {
          var d = _data[i];
          sqrSum += (d - avg) * (d - avg);
        }

        return Math.Sqrt(sqrSum / _length);
      }
    }

    public double Average(Func<double, double> func) {
      return Sum(func) / _length;
    }

    public double Sum(Func<double, double> func) {
      var sum = 0.0;
      for (var i = 0; i < _length; i++) sum += func(_data[i]);

      return sum;
    }

    public bool Any(Predicate<int> predicate) {
      for (var i = 0; i < _length; i++)
        if (predicate(i))
          return true;

      return false;
    }

    public void CopyFrom(Series s) {
      for (var i = 0; i < _length; i++) _data[i] = s._data[i];
    }

    public Series Clone() {
      var s = new Series(_length);
      s.CopyFrom(this);
      return s;
    }

    public void Reset() {
      for (var i = 0; i < _length; i++) _data[i] = 0.0;
    }

    // 将array各项累加到Series对应项
    public void Add(Series s) {
      for (var i = 0; i < _data.Length; i++) _data[i] += s._data[i];
    }

    public void Subtract(Series s) {
      for (var i = 0; i < _data.Length; i++) _data[i] -= s._data[i]; //这里不做超限检查
    }

    // 从总容量capacity中减去s，将this对应的值设置为差值
    public void SubtractByCapacity(Series capacity, Series s) {
      for (var i = 0; i < _data.Length; i++) _data[i] = capacity._data[i] - s._data[i]; //这里不做超限检查
    }

    // 计算 当前序列 this + s 之后 向量的最大值
    // 不会修改当前序列 
    public double MaxWith(Series s) {
      var v = double.MinValue;
      for (var i = 0; i < _length; i++) {
        var d = _data[i] + s[i];
        if (v < d) v = d;
      }

      return v;
    }

    public static Series Parse(string[] fields) {
      var s = new Series(fields.Length);
      for (var i = 0; i < fields.Length; i++) s._data[i] = double.Parse(fields[i]);

      return s;
    }

    public override string ToString() {
      var s = new StringBuilder();

      foreach (var i in _data) s.Append($"{i:0.00},");

      return s.Length > 1 ? s.ToString(0, s.Length - 1) : string.Empty;
    }
  }
}