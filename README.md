## Usage

### Lower Bound

```bash
g++ code/lower_bound.cpp -O2 -o lb && ./lb < request > search
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

### Generate data/submit.csv

```bash
python code/build_submit.py --search=search-result/search_xxx   # the result will be outputed to data/submit.csv
```


### 检查搜索结果
```bash
dotnet judge/verifysearch.dll search6991 # hard coded [search-result] path

python code/main.py --data_dir=data --method=3 --search=search-result/search6278 --larger_cpu_util=[default=1] --smaller_cpu_util=[default=1]
```

### 官方评分 
```bash

java -jar judge/judge.jar data/b.csv submit.csv # Decompress the data/b.csv.tgz first !!!

```

### 运行dotnet
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

# 数据集 B 2018-07-26

## 机器

**集群共6000台机器，分为下面2种配置，各3000台。**

Disk 规格变大了，不再是紧缺资源。

| cpu | mem  | disk | p   | m   | pm  | cnt  | cpu/mem |
| --- | ---: | ---: | --- | --- | --- | ---- | :-----: |
| 32  | 64   | 1440 | 7   | 3   | 7   | 3000 | 0.50    |
| 92  | 288  | 2457 | 7   | 7   | 9   | 3000 | 0.32    |

资源总量为：
CPU  ：  372,000 个核 ； Mem  ： 1,056,000 GB 

统计详情可见 [init_deploy_b.csv](init_deploy_b.csv) 和 [app_util_b.csv](app_util_b.csv)。
## 应用和实例

**共有 9338 个应用， 68224 个实例。**
申请资源占总容量的比例：
```
        Total,   Util
Disk: 4567563, 39.07%
P   :    1465,  3.49%
M   :       0,  0.00%
PM  :    1465,  3.05%
```

CPU: Max 55.1%, Min 31.5%, Avg 45.3%

Mem: Max 60.0%, Min 58.7%, Avg 59.6%

![CPU和内存资源利用率](/util_b.png)
