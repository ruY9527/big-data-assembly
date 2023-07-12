package com.iyang.utils;

import java.util.ResourceBundle;

/****
 * author: BaoYang
 * date: 2023/7/12
 * desc:
 ***/
public class PropertiesUtil {

    private static ResourceBundle config = ResourceBundle.getBundle("config");

    public static String getProperty(String name)  {

        return  config.getString(name);

    }

    public static void main(String[] args) {

        System.out.println(PropertiesUtil.getProperty("redis.host"));

    }

}
