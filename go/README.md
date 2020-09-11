## 执行方法：
- 解压数据集：将`../data/semifinal_data_20180815.zip`解压到`../data/scheduling_semifinal_data_20180815`
- 运行代码：
```
# 搜索结果会保存在submit_<method>search_<dataSet>.csv
# 默认会用submit_localsearch_a.csv文件，使用1核对数据集a进行第一轮搜索
$ ./run.sh <submit_file> <cores> <dataSet> <round> <method>
```
其中 `submit_file`指的是搜索之后生成的instance向machine迁移的文件；`cores`指的是搜索过程用到的核心数；`<dataSet>` 指的是数据集，在复赛中提供了 `a,b,c,d,e`共五套数据集，位于`../data/scheduling_semifinal_data_20180815/`中；`round`表示instance迁移的轮次，可取`1 2 3`三个值；`method`表示使用的搜索方法，可取`local tabu analyse reduce`四个值。