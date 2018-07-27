using System;
using System.Linq;
using System.Text;

namespace Tianchi {
  public class Series {
    private readonly double[] _data;

    public Series(int length) {
      _data = new double[length];
    }

    public Series(int length, double initValue) : this(length) {
      for (var i = 0; i < length; i++) _data[i] = initValue;
    }

    public double this[int i] => _data[i];

    public double Max => _data.Max();

    public double Min => _data.Min();

    public double Avg => _data.Average();

    public double Stdev {
      get {
        var avg = Avg;
        return Math.Sqrt(_data.Average(i => (i - avg) * (i - avg)));
      }
    }

    // 将array各项累加到Series对应项
    public void Add(Series s) {
      for (var i = 0; i < _data.Length; i++) _data[i] = _data[i] + s._data[i];
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
      return _data.Select((t, i) => t + s[i]).Max();
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