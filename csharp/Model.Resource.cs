namespace Tianchi {
  public class Resource {
    public const int TsCount = 98;

    public Resource() {
      Cpu = new Series(TsCount);
      Mem = new Series(TsCount);
    }

    public Resource(Series cpu, Series mem, int disk, int p, int m, int pm) {
      Cpu = cpu;
      Mem = mem;
      Disk = disk;
      P = p;
      M = m;
      Pm = pm;
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

    public Resource Copy(Resource r) {
      Cpu.Copy(r.Cpu);
      Mem.Copy(r.Mem);
      Disk = r.Disk;
      P = r.P;
      M = r.M;
      Pm = r.Pm;
      return this;
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

    public Resource Add(Resource r) {
      Cpu.Add(r.Cpu);
      Mem.Add(r.Mem);
      Disk += r.Disk;
      P += r.P;
      M += r.M;
      Pm += r.Pm;
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

    // 计算总容量 capacity 与 r 的差值，分别赋给 this 对应的维度
    // 即 this = capacity - r
    public Resource SubtractByCapacity(Resource capacity, Resource r) {
      Cpu.SubtractByCapacity(capacity.Cpu, r.Cpu);
      Mem.SubtractByCapacity(capacity.Mem, r.Mem);
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