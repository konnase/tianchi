namespace Tianchi {
  public class Resource {
    public const int T1470 = 1470; // 兼容：复赛使用1470个数据点
    public const int T98 = 98;
    public const int Interval = T1470 / T98;

    /// <summary>
    ///   cpu或mem的数据点长度
    /// </summary>
    private int _length;

    public Resource(bool isT1470 = true) {
      if (isT1470) {
        Cpu = new Series(T1470);
        Mem = new Series(T1470);
        _length = T1470;
      } else {
        Cpu = new Series(T98);
        Mem = new Series(T98);
        _length = T98;
      }
    }

    public Resource(Series cpu, Series mem, int disk, int p, int m, int pm) {
      //要求Cpu和Mem维度相同
      Cpu = cpu;
      Mem = mem;
      Disk = disk;
      P = p;
      M = m;
      Pm = pm;
      _length = cpu.Length;
    }

    public bool IsValid => Disk != int.MinValue;

    public Series Cpu { get; }
    public Series Mem { get; }
    public int Disk { get; private set; }
    public int P { get; private set; }
    public int M { get; private set; }
    public int Pm { get; private set; }

    public Resource Invalid() {
      Disk = int.MinValue;
      return this;
    }

    public Resource CopyFrom(Resource r) { // 兼容：支持从98维拷贝到1470维
      Cpu.CopyFrom(r.Cpu);
      Mem.CopyFrom(r.Mem);
      Disk = r.Disk;
      P = r.P;
      M = r.M;
      Pm = r.Pm;
      _length = r._length;
      return this;
    }

    public Resource Clone() {
      var r = new Resource(_length == T1470);
      r.CopyFrom(this);
      return r;
    }

    public Resource Reset() {
      Cpu.Reset();
      Mem.Reset();
      Disk = 0;
      P = 0;
      M = 0;
      Pm = 0;
      return this;
    }

    //支持不同维度资源相加 1470 + 98
    public Resource Add(Resource r) {
      Cpu.Add(r.Cpu);
      Mem.Add(r.Mem);
      Disk += r.Disk;
      P += r.P;
      M += r.M;
      Pm += r.Pm;
      return this;
    }

    public Resource Add(JobBatch batch) {
      Cpu.Add(batch.Cpu * batch.Size, batch.StartTime, batch.Duration);
      Mem.Add(batch.Mem * batch.Size, batch.StartTime, batch.Duration);
      return this;
    }

    public Resource Subtract(JobBatch batch) {
      Cpu.Subtract(batch.Cpu * batch.Size, batch.StartTime, batch.Duration);
      Mem.Subtract(batch.Mem * batch.Size, batch.StartTime, batch.Duration);
      return this;
    }

    //注意，返回的不是新的对象，而是 a，因而连续的表达式是从左到右结合的
    //即 a-b+c 相当于 a.Subtract(b).Add(c)，每一次计算都会修改 a 的值！
    //public static Resource operator +(Resource a, Resource b) => a.Add(b);

    // 从当前资源中扣除 r,
    // 这里不做超限检查
    public Resource Subtract(Resource r) {
      Cpu.Subtract(r.Cpu);
      Mem.Subtract(r.Mem);
      Disk -= r.Disk;
      P -= r.P;
      M -= r.M;
      Pm -= r.Pm;
      return this;
    }

    //注意，返回的不是新的对象，而是 a，因而连续的表达式是从左到右结合的
    //public static Resource operator -(Resource a, Resource b) => a.Subtract(b);

    // 计算总容量 capacity 与 r 的差值，赋给 this 对应的维度
    // 即 this = capacity - r
    public Resource DiffOf(Resource capacity, Resource r) {
      Cpu.DiffOf(capacity.Cpu, r.Cpu);
      Mem.DiffOf(capacity.Mem, r.Mem);
      Disk = capacity.Disk - r.Disk;
      P = capacity.P - r.P;
      M = capacity.M - r.M;
      Pm = capacity.Pm - r.Pm;
      return this;
    }

    public bool IsOverCap(Resource capacity) {
      return Disk > capacity.Disk
             || P > capacity.P
             || Pm > capacity.Pm
             || M > capacity.M
             || Cpu.Any(i => Cpu[i] > capacity.Cpu[i]) //TODO: Round
             || Mem.Any(i => Mem[i] > capacity.Mem[i]);
    }

    public override string ToString() {
      return $"{Cpu.Max:0.0},{Mem.Max:0.0},{Disk},{P}";
    }
  }
}
