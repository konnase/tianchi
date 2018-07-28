# 执行官方评分程序

在当前目录（`somepath/tianchi/judge/`）执行

```
java -jar judge.jar \
../data/scheduling_preliminary_b_app_resources_20180726.csv \
../data/scheduling_preliminary_b_machine_resources_20180726.csv \
../data/scheduling_preliminary_b_instance_deploy_20180726.csv \
../data/scheduling_preliminary_b_app_interference_20180726.csv \
../submit.csv
```

或
```
java -jar judge.jar ../data/b.csv ../submit.csv
```

> 其中 `b.csv` 是按评分程序格式合并的数据。

# 参考官方评分程序，修改的Bugs
1. 官方程序对初始部署中不满足约束的那些实例的处理措施是：
    1. 忽略资源额度和亲和性冲突，直接将其部署到机器上
    2. 开始评分后，仅当遇到该实例，才将其从旧机器上移除，并部署到提交结果指定的新机器上
    3. 这样在执行到该实例之前，可能会向对应的旧机器部署其它实例，产生冲突

    ying的C#代码在读入初始部署时，直接跳过这些无法部署的，这种做法简化了问题，但不符合官方的做法。

2. 之前是执行完部署算法后遍历机器，再遍历机器上的部署实例，输出到文件。这样无法保证输出正确的执行顺序。改为每添加一个实例，即输出一条记录。

3. 检查亲和性约束的代码有一个k=0的边界条件Bug，还有一个不对称规则Bug。