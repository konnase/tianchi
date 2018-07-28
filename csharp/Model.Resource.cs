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

    public void Add(Resource r) {
      Cpu.Add(r.Cpu);
      Mem.Add(r.Mem);
      Disk += r.Disk;
      P += r.P;
      M += r.M;
      Pm += r.Pm;
    }

    // 从当前资源中扣除 r ,
    // 这里不做超限检查
    public void Subtract(Resource r) {
      Cpu.Subtract(r.Cpu);
      Mem.Subtract(r.Mem);
      Disk -= r.Disk;
      P -= r.P;
      M -= r.M;
      Pm -= r.Pm;
    }

    // 计算总容量 capacity 与 r 的差值，分别赋给 this 对应的维度
    public void SubtractByCapacity(Resource capacity, Resource r) {
      Cpu.SubtractByCapacity(capacity.Cpu, r.Cpu);
      Mem.SubtractByCapacity(capacity.Mem, r.Mem);
      Disk = capacity.Disk - r.Disk;
      P = capacity.P - r.P;
      M = capacity.M - r.M;
      Pm = capacity.Pm - r.Pm;
    }

    public override string ToString() {
      return $"{Cpu.Max:0.0},{Mem.Max:0.0},{Disk},{P}";
    }
  }
}