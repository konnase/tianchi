## 修改为阿里的Maven镜像源
修改Maven的配置文件，
+ Windows：`C:\App\maven\conf\settings.xml`，
+ Ubuntu：`/etc/maven/settings.xml`
 
修改其中`<mirrors>...</mirrors>`部分，改成

```
<mirrors>
  <mirror>
    <id>alimaven</id>
    <mirrorOf>central</mirrorOf>
    <name>aliyun maven</name>
    <url>https://maven.aliyun.com/nexus/content/groups/public/</url>
  </mirror>
</mirrors>
```

# 使用Maven构建jar
在命令行，切换到`judge/`目录下，执行`mvn package`，开始编译。
第一次执行`maven`会下载很多依赖库，稍等几分钟（如果没有修改为阿里的源，则可能需要半个小时）。

编译结果是 `target/judge-semifinal_20180827.jar` 。可以把这个`jar`重命名，或拷贝到数据目录，方便敲命令。

# 执行官方评分程序

```
tianchi/
├── data
│   ├── app_interference.csv
│   ├── app_resources.csv
│   ├── instance_deploy.a.csv
│   ├── instance_deploy.b.csv
│   ├── instance_deploy.c.csv
│   ├── instance_deploy.d.csv
│   ├── instance_deploy.e.csv
│   ├── job_info.a.csv
│   ├── job_info.b.csv
│   ├── job_info.c.csv
│   ├── job_info.d.csv
│   ├── job_info.e.csv
│   ├── machine_resources.a.csv
│   ├── machine_resources.b.csv
│   ├── machine_resources.c.csv
│   ├── machine_resources.d.csv
│   ├── machine_resources.e.csv
│   └── problem.a.csv
├── judge.jar
└── a.csv

```

按下面的命令格式执行评分程序：
```
java -jar judge.jar \
data/app_resources.csv data/machine_resources.a.csv data/instance_deploy.a.csv data/job_info.a.csv app_interference.csv \
a.csv
```

或
```
java -jar judge.jar data/problem.a.csv a.csv
```

> 其中`a.csv`是提交结果， `problem.a.csv` 是按评分程序格式合并的数据。
