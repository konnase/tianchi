## Usage

### Lower Bound

```bash
g++ code/lower_bound.cpp -o lb && ./lb < request > search
```

rating for this lower_bound:

```bash
python code/main.py --data_dir=data --method=2 --test=lower_bound
```

### Solution

```bash
$ python code/main.py --data_dir=data --method=<method>
```

Method enum:

```python
class Method(Enum):
    FFD = 1
    Knapsack = 2
    Analyse = 3
```

### 官方评分 
```bash

java -jar judge/judge.jar data/problem.csv submit.csv

# suppose there is the submit.csv file at project root
```


### 导出最终提交文件 submit.csv
```bash
dotnet run --project csharp/tianchi.csproj  ./
# 终端输出结束后，提交文件保存在了 submit.csv

# 或者在 cssharp 目录执行
cd csharp
dotnet run ../
```
> 注意： `.gitignore`中忽略了 `submit*` ；如果需要添加 `submit.csv` 到仓库，请使用 `git add -f submit.csv`
> 
-----

阿里巴巴全球调度算法大赛
https://tianchi.aliyun.com/competition/information.htm?raceId=231663

-----

共 4 个csv数据文件，分别为
+ 应用 app（`aid, cpu, mem, disk, P, M, PM`）
+ 机器 m（`mdi, cpu, mem, disk, P, M, PM`）  
+ 实例部署 deploy（`nid, aid, mid`）
+ 干扰约束 x（`aid, bid, k`）

## 机器

**集群共6000台机器，分为下面2种配置，各3000台。**


数据集 **b** ，disk 规格变大了
| cpu | mem | disk | p | m | pm | cnt  | cpu/mem |
|-----|----:|-----:|---|---|----|------|:-------:|
| 32  | 64  | 1440 | 7 | 3 | 7  | 3000 | 0.50    |
| 92  | 288 | 2457 | 7 | 7 | 9  | 3000 | 0.32    |


数据集 **a**
| cpu | mem | disk | p | m | pm | cnt  | cpu/mem |
|-----|----:|-----:|---|---|----|------|:-------:|
| 32  | 64  |  600 | 7 | 3 | 7  | 3000 | 0.50    |
| 92  | 288 | 1024 | 7 | 7 | 9  | 3000 | 0.32    |

资源总量为：
CPU  ：  372,000 个核 ； Mem  ： 1,056,000 GB 

### 初始部署
初始有 2515 台机器已经部署了实例，分别为： 占用 600 GB 硬盘的机器 1276 台，空闲 1724 台，占用 1024 GB 硬盘的机器 1239 台，空闲 1761 台。
有 117 台机器存在少数几个实例违反了亲和约束，其中 600 GB 硬盘的机器 32 台；1024 GB 硬盘的机器 85 台。
没有实例违反资源约束。

需要注意的是，
+ **初始部署的装箱效率还是很高的，特别是最紧张的 disk 资源，利用率基本都在 90% 以上，甚至 100% ；**
+ 分机器类型看，**600 GB 硬盘的机器装箱存在过量，即有 423 台此类型机器的成本分数 > 1.0；而 1024 GB 硬盘的机器只有 1 台成本分数 > 1.0。**

统计详情可见 [init_deploy_machine_util_stat.csv](init_deploy_machine_util_stat.csv) 和 `init_deploy_machine_util.xlsx`

这也许就是 **比赛官方比较希望改进的地方吧。**
**也许大部分初始部署可以按兵不动，但需要对 600 GB硬盘的机器特殊处理。**

## 应用和实例

这里的应用[是指持续运行的服务](https://tianchi.aliyun.com/forum/new_articleDetail.html?raceId=231663&postsId=5338)，而每个实例是指一个（docker）容器。记实例`id`为`nid`。

**共有 9338 个应用， 68219 个实例。**

### 应用的实例个数分布
 
应用的大小（实例个数）分布如下：
+ 平均每个应用有 7.3 个实例，最大的应用有 610 个实例，最小的应用只有 1 个实例
+ 实例个数多于 10 个的应用仅占 **应用** 个数约10%（1061个），实例个数多于 100 个的应用比例仅约 1%（104个）

应用的实例个数分布是显著不均衡的。

### P, M和PM资源
没有明确P，M和PM这三类资源具体是什么。
所有应用的 M 均为0。
且P与PM的值均相同，取值为0或1，即同时为0（66754个实例，绝大部分应用）或同时为1（1465个实例，11个应用）。
P = 1 的应用对应 Disk 需求绝大部分为 60 （1335个实例，属10个应用）， 有 1 个应用（130个实例）的 Disk 为 80 。

所有机器的P均为7，PM有7和9两种。所以 P，M 和 PM 对大部分应用实例来是不算紧缺的资源。

### 硬盘资源
合计各应用实例的硬盘资源需求，共需 4,563,191 GB，而6000台机器的硬盘总容量有 4,872,000 GB，**居然达到了 93.7 %， 比CPU和内存还紧张。**

磁盘资源需求的分布从40 GB 到 1024 GB共 16 个离散值，集中在 60 GB，占总实例个数的 78.5 %。
大于等于 600 GB 的实例个数有 41 个，占个数的 0.06 %。

### CPU和内存资源

CPU和内存 资源曲线是一天 24小时 内对应用各实例定时采样后汇总的平均值，共98个采样点，两点的间隔约 15min。

按 ts 时刻，合计所有 应用实例的 CPU和内存 资源请求，计算利用率。

|      | CPU使用量 / 利用率 | 内存使用量 / 利用率 |
|------|------------------:|-------------------|
| 最大 | 112989.84 / 30.4% | 405566.90 / 38.4% |
| 最小 |  68439.36 / 18.4% | 375839.43 / 35.6% |
| 平均 |  93850.33 / 25.3% | 396890.45 / 37.6% |

![CPU和内存资源利用率](/utilts.jpg)

CPU有日内峰谷的整体趋势，但具体到实例，分时的变化曲线就可能不太规律了。

对CPU，stdev < 0.5 的 共有 46048 个实例，8613 个应用；
对内存，stdev < 0.5 的 共有 50095 个实例，7283 个应用。
即大部分应用的 CPU和内存 几乎没有波动。

当然，初赛对迁移做了简化，暂时不必深入分时曲线。
