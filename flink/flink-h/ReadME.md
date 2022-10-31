## 介绍
Flink是一个框架和分布式处理引擎,用于在无边界和有边界数据流上进行有状态的计算.Flink能在所有常见集群环境中运行,并能以内存的速度和任意规模进行计算.
1. 无界流: 实时流,有定义的开始,但是没有定义流的结束.它会无休止的产生数据.
2. 有界流: 有定义的开始,也有定义流的结束.有界流: 批处理.
## 目标
1.Flink编程模型
2.Flink数据传输方式
3.Flink并行原理
## 部署方式
1. HADOOP YARN
2. Apache Mesos
3. K8S
## 运行模式
可以运行任意规模应用
## Flink相关知识
1. 主从架构, 主节点交 JobManager,从节点叫TaskManager
## 数据传输策略
1. forward strategy
1.1 一个Task的输出只发送一个task作为输入
1.2 如果两个task都在一个JVM中的话,就可以避免网络开销
2. key based strategy
2.1 数据需要按照某个属性(我们称为key)进行分组(或者分区)
2.2 相同key的数据需要传输给同一个task,在一个task中进行处理
3. broadcast strategy
4. random strategy
4.1 数据随机的从一个task中传输给下一个operator所有的subtask
4.2 保证数据能均匀的传输给所有的task
TaskManager 并行度 Task Task的数据传输策略
## Flink分布式运行
flink开发环境需要构建dataflow,这个dataflow运行需要经历4个阶段,
1. Stream Graph
2. Job Graph
3. Execution Graph
4. Physical Exectuion Graph
## JobManager
    作业管理器
## TaskManager
    任务管理器
## 概念知识
1. dataflow graph: 数据流图
2. JobGraph: 作业图, 执行图(ExecutionGraph)
## 提交作业模式
1. 会话模式
2. 单作业模式
3. 应用模式
## 场景介绍
1. 电商和市场营销: 实时数据报表,广告投放,实时推荐
2. 物联网: IOT,传感器实时数据采集等
3. 物流配送和服务业: 订单状态实时更新,通知信息推荐
4. 银行和金融业: 实时结算和通知推送,实时检查异常行为