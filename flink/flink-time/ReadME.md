#时间语义
## 事件时间(一个是数据产生时间)
## 处理时间(数据真正被处理的时间)
## 摄入时间()
## 函数
1. ProcessFunction: 最基本的函数,基于DataStream直接调用.process()时作为参数传入
2. KeyedProcessFunction: 对流按键分区后的处理函数,基于KeyedStream调用.process()时作为参数传入
3. ProcessWindowFunction: 开窗之后的处理函数,也是全窗口函数的代表。
4. ProcessAllWindowFunction: 同样是开窗之后的处理函数,基于AllWindowStream调用.process()时作为参数传入
5. CoProcessFunction: 合并(connect)两条流之后的处理函数,基于ConnectedStreams调用.process()时作为参数传入
6. ProcessJoinFunction: 间隔连接(interval join)两条流之后的处理函数，基于InteralJoined调用.process()时作为参数传入
7. BroadcastProcessFunction: 广播连接流处理函数,基于BroadcastConnectedStream调用.process()时作为参数传入。
8. KeyedBroadcastProcessFunction: 按键分区的广播连接流处理函数,同样是基于BroadcastConnectedStream调用.process()时作为参数传入。
## 水位线
不依赖系统时间,而是基于数据自带的时间戳去定义一个时钟,用来表示当前的进展.
水位线 = 观察到的最大事件时间 - 最大延迟时间 - 1毫秒
## 窗口
1. 时间窗口(Time Window)
2. 计数窗口(Count Window)
3. 滚动窗口(Tumbling Windows)
4. 滑动窗口(Sliding Windows)
5. 会话窗口(Session Windows)
6. 全局窗口(Global Windows)
窗口分配器(Window Assigners)和窗口函数(Window Functions)
## 增量聚合和全窗口函数
1. 增量聚合函数处理计算会更高效. 增量聚合计算量"均摊"到了窗口收集数据的过程中,自然就会比全窗口聚合更加高效，输出更加实时。
2. 全量窗口优势在于提供了更多的信息，可认为是更加"通用"的窗口操作
## 窗口的生命周期
1. 窗口的创建
2. 窗口计算的触发: 窗口函数(window functions)和触发器(trigger)
3. 窗口的销毁: 当时间达到了结束点后,就会直接触发计算输出结果,进而清除状态销毁窗口
## 迟到数据处理(late data)
1. 设置水位线延迟时间
2. 容许窗口处理迟到数据
3. 将迟到数据放入窗口测流输出: 兜底方法。窗口真正关闭，所以无法基于之前窗口的结果直接做更新。只能保存之前窗口的结果，然后获取侧边输出流中迟到数据，判断数据所属窗口，手动对结果进行合并更新，虽然实时性不强，但是可以保证结果的一致性。
## 背压
数据产生的时间和处理的时间可能是完全不同的,很长时间收集起来的数据,处理或许只需一瞬间;
也有可能数据量过大,处理能力不足,短时间堆了大量数据处理不完,产生"背压"(back pressure)
## 处理流程
1. 读取数据源
2. 选择浏览行为(PV)
3. 提取时间戳并生成水位线
4. 按照url进行keyBy分区操作
5. 开长度为1小时,步长为5分钟的事件事件滑动窗口
6. 使用增量聚合函数AggregateFunction,并结合全窗口函数WindinFunction进行窗口聚合,得到每个url,在每个统计窗口内的浏览量,包装成UrlViewCount计算
7. 按照窗口进行keyBy分区操作
8. 对同一窗口的统计结果数据,使用KeyedProcessFunction进行收集并排序输出
