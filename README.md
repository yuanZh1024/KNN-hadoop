# KNN模型实现电影网站用户性别预测
KNN，典型的监督式学习算法。在此使用Hadoop，利用MapReduce进行分布式计算。
（数据在data目录下，有些数据缺失用户性别）
### 关于运行
代码中使用Toolrunner，所以不用生成jar包再使用hadoop jar提交任务，只需导入IDE中，对main函数直接RUN AS JavaApplication中即可
# 实现步骤
## **1.数据Join**
连接users/movies/ratings
## **2.统计用户看过的电影类型Genres**
Map输出：
```
key: <UserID,Gender,Age,Occupation,Zip-code>
value: <Genres>
```
Reduce实现：
HashMap<电影类型，电影类型数目>
```java
HashMap<String,Integer> genresCounts=new HashMap<String,Integer>();
String[] genreslist={"Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama",
				"Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"		
				};
```
通过遍历Map输出的values 统计genres类型数目
提取hashmap中的vals 组成String
Reduce输出：
```
key: <UserID,Gender,Age,Occupation,Zip-cod,对应电影类型数量,...>
value: NullWritable
```
生成特征向量。
## **3.数据清洗Processing**
dataProcessing
- 异常值
- 空值
噪声数据：缺失值 异常值
处理方法：删除记录/数据插补
##### 插补方法：
- 1.均值/中位数/众数插补
- 2.固定值
- 3.最近邻插补
- 4.回归方法（拟合模型预测）
- 5.插值函数 f(x1)
##### 异常值处理：
- 1.删除
- 2.视为缺失值
- 3.平均值修正

代码中设置计数器,将会输出
```
com.yuanzh.dataprocessing.DataProcessingMapper$DataProcessingCounter
		AbnormalData=0
		NullData=0
```

## **4.划分数据集**
- 1.训练样本集 80%
- 2.验证数据集10%——选择最优模型参数 
- 3.测试样本集10%——评估分类器准确性
## 5.用KNN模型实现分类
距离采用欧式距离，k暂时取3
## 6.模型评价
准确率=正确识别的总体个数/识别出的个体总数
## 7.选取最优k值
理论上，k值越小越容易过拟合。
运行alljob（选取不同k值进行预测分类，最终得到最高的准确率对应的k），得到k=3 accuracy=0.77
### 涉及命令
```
hdfs dfs -put data/movies.dat /movie/movies.dat
hdfs dfs -put data/users.dat /movie/users.dat
hdfs dfs -put data/ratings.dat /movie/ratings.dat
 hdfs dfs -mkdir /movie/ratings_users
 hdfs dfs -mkdir /movie/trainData
 hdfs dfs -mkdir /movie/testData
 hdfs dfs -mkdir /movie/validateData
```
