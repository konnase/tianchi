namespace Tianchi {
  public class Instance {
    public readonly App App;
    public readonly int Id;
    public bool Deployed = false;

    public Machine Machine;

    public Instance(int id, App app) {
      Id = id;
      App = app;
    }

    public Resource R => App.R;

    public Instance Clone() {
      return new Instance(Id, App); //只有machine在不同的solution是不同的对象
    }

    public override string ToString() {
      return $"inst_{Id},app_{App.Id},{R}";
    }
  }
}