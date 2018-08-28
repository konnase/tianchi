# 安装JDK和Maven
JDK 下载页 [http://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html]
Maven 下载页 [https://maven.apache.org/download.cgi]

## Windows 10 x64
+ 在JDK下载页下载 [jdk-8u181-windows-x64.exe](http://download.oracle.com/otn-pub/java/jdk/8u181-b13/96a7b8442fe848ef90c96a2fad6ed6d1/jdk-8u181-windows-x64.exe)，然后安装到默认目录`C:\Program Files\Java\jdk1.8.0_181\`。
+ 在Maven下载页下载 [apache-maven-3.5.4-bin.zip](http://www-us.apache.org/dist/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.zip)，解压到并修改目录名为`C:\App\maven\`。
+ 按`Win+S`快捷键，搜索“环境变量”，打开`环境变量`对话框，
    - **编辑** `系统变量` 中的`Path`，增加 `C:\Program Files\Java\jdk1.8.0_181\bin` 和 `C:\App\Maven\bin` 两个值
    - 在`系统变量`中 **新建** ，变量名为`JAVA_HOME`，变量值为 `C:\Program Files\Java\jdk1.8.0_181`
  
## Ubuntu 18.04 
下面两步要按顺序执行，否则Maven会稍带安装 `jre-10`。
+ 先安装JDK，执行 `sudo apt install -y openjdk-8-jdk-headless`
+ 再安装Maven，执行 `sudo apt install -y maven`

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
将`judge.zip` 解压到 如`C:\Users\ying\judge`或`/home/ying/judge`。

这个压缩包是按Maven项目格式组织的，
+ `pom.xml`是Maven的构建文件，
+ `src/main/java/AlibabaSchedulerEvaluatorRun.java` 是从官方下载的`AlibabaSchedulerEvaluatorRun_20180709.java` ，仅修改了文件名，没有改动文件内容。
+ 将代码文件拷贝到 `src/main/java/`，这是`maven`默认的项目结构；
+ 将代码文件重命名为 `AlibabaSchedulerEvaluatorRun.java` ，是 `javac` 的要求，即代码文件名要与代码中的 **公开类名** 一致。

在命令行，切换到该目录下，执行`mvn package`，开始编译。
第一次执行`maven`会下载很多依赖库，稍等几分钟（如果没有修改为阿里的源，则可能需要半个小时）。

编译产出是 `target/judge-semifinal_20180827.jar` 。可以把这个`jar`重命名，或拷贝到数据目录，方便敲命令。


# 执行官方评分程序

假设当前工作目录结构为：
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
│   └── machine_resources.e.csv
├── judge.jar
└── submit.csv

```
因为没有修改官方代码，不要在意它的提示信息。
按下面的命令格式执行评分程序：
```
java -jar judge.jar \
data/app_resources.csv data/machine_resources.a.csv data/instance_deploy.a.csv data/job_info.a.csv app_interference.csv \
submit.csv
```

或
```
java -jar judge.jar data/semifinal.csv submit.csv
```

> 其中`submit.csv`是提交结果， `semifinal.csv` 是按评分程序格式合并的数据。
