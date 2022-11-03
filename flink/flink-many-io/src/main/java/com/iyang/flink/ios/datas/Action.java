package com.iyang.flink.ios.datas;

/***
 * @author: baoyang
 * @data: 2022/11/3
 * @desc:
 ***/
public class Action {

    public String userId;
    public String action;

    public Action() {
    }

    public Action(String userId, String action) {
        this.userId = userId;
        this.action = action;
    }

    @Override
    public String toString() {
        return "Action{" +
                "userId='" + userId + '\'' +
                ", action='" + action + '\'' +
                '}';
    }
}
