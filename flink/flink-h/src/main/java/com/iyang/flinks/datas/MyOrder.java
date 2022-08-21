package com.iyang.flinks.datas;

/***
 * @author: baoyang
 * @data: 2022/8/7
 * @desc:
 ***/
public class MyOrder {

    public Long id;
    public String product;
    public int amount;

    public MyOrder(){}

    public MyOrder(Long id, String product, int amount) {
        this.id = id;
        this.product = product;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "MyOrder{" +
                "id=" + id +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                '}';
    }
}
