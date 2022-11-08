package com.iyang.lambda.exp1;

import java.util.Optional;

/***
 * @author: baoyang
 * @data: 2022/11/8
 * @desc:
 ***/
public class OptionalExp {

    public static void main(String[] args) {

        Optional<String> empty = Optional.<String>empty();
        // Exception in thread "main" java.util.NoSuchElementException: No value present
        String s = empty.orElseGet(() -> "zhangsan");
        System.out.println(s);
    }

}
