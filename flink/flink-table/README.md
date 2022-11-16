## 创建表环境
1. 注册catalog和表
2. 执行SQL查询
3. 注册用户自定义函数(UDF)
4. DataStream和表之间转换

## 创建表
1. 连接器表(Connector Tables)
2. 虚拟表

## 动态表转化为流
1. 仅追加(Appned-only)流
- 仅通过插入(Insert)更改来修改动态表,可以直接转化为"仅追加"流。这个流中发出的数据，其实就是动态表中新增的每一行
2. 撤回(Retract)流
- 撤回流包含两类消息, 添加(add)消息和撤回(retract)消息
- INSERT插入操作编码为add消息,DELETE删除操作为编码retract消息;而UPDATE更新操作则编码为被更改行的retract消息，和更新后(新行)的add消息。
3. 更新插入(Upsert)流
- 更新插入流中包含两种类型的消息,更新插入(upsert)消息和删除(delete)消息
- "upsert"其实是"update"和"insert"的合成词,所以对于更新插入流来说,INSERT插入操作和UPDATE更新操作,统一被编码为upsert消息;而delete删除操作则编码为delete消息

## 时间语义
1. 事件事件
2. 处理事件
ts设置为事件事件属性,而基于ts设置了5s的水位延迟线,这里的5s是以时间间隔的形式定义的,格式是INTERVAL<数值><单位>
```$xslt
CREATE TABLE EventTable(
 user STRING,
 url STRING,
 ts TIMESTAMP(3),
 WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
 ...
);
```