namespace Tianchi {
  public class Instance {
    public readonly App App;
    public readonly int Id;

    public Machine DeployedMachine;

    public bool NeedDeployOrMigrate = true;

    public Instance(int id, App app) {
      Id = id;
      App = app;
    }

    public Resource R => App.R;

    public override string ToString() {
      return $"inst_{Id},app_{App.Id}, {R}";
    }
  }
}