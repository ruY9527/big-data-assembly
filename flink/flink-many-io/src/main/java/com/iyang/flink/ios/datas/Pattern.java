package com.iyang.flink.ios.datas;

/***
 * @author: baoyang
 * @data: 2022/11/3
 * @desc:
 ***/
public class Pattern {

    public String action1;
    public String action2;

    public Pattern() {
    }

    public Pattern(String action1, String action2) {
        this.action1 = action1;
        this.action2 = action2;
    }

    @Override
    public String toString() {
        return "Pattern{" +
                "action1='" + action1 + '\'' +
                ", action2='" + action2 + '\'' +
                '}';
    }
}
