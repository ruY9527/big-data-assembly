## FuncitonInterface注解注释
## 继承小窍门
1. 类胜于接口。如果在继承链中有方法体或者抽象的方法声明,那么就可以忽略接口中定义的方法
2. 子类胜于父类。如果一个接口继承另外一个接口，且两个接口都定义了一个默认方法，那么子类中定义的方法胜出
3. 如果1和2规则不适用,子类要么需要实现该方法,要么将该方法声明为抽象方法