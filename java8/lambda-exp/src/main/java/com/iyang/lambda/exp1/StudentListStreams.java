package com.iyang.lambda.exp1;

import cn.hutool.core.util.NumberUtil;
import com.iyang.pojo.Student;
import com.iyang.pojo.StudentFactory;
import com.iyang.pojo.Track;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/***
 * @author: baoyang
 * @data: 2022/11/8
 * @desc:
 ***/
public class StudentListStreams {

    public static void main(String[] args) {

        // toListExp();
        //toMapExp();
        //toFilterExp();
        //toFlatMapExp();
        //maxNumber();
        toReduceExp();
    }

    public static void toReduceExp(){

        Integer reduce = Stream.of(1, 2, 3)
                .reduce(0, (acc, element) -> acc + element);
        System.out.println(reduce);

    }

    public static void maxNumber(){

        List<Track> trackList = Arrays.asList(
                new Track("lisi", 233),
                new Track("zs", 524),
                new Track("ts", 451)
        );
        Track track1 = trackList.stream().min(Comparator.comparing(track -> track.name.length())).get();

        System.out.println(track1.toString());
    }

    public static void toFlatMapExp() {

        List<Integer> integerList = Stream.of(Arrays.asList(1, 2), Arrays.asList(3, 4))
                .flatMap(numbers -> numbers.stream())
                .collect(Collectors.toList());

        String string = Arrays.toString(integerList.toArray());
        System.out.println(string);
    }


    public static void toFilterExp(){

        List<String> numberList = Stream.of("a", "1bc", "abc1").filter(s -> NumberUtil.isInteger(s.charAt(0) + "")).collect(Collectors.toList());
        System.out.println(Arrays.toString(numberList.toArray()));

    }

    public static void toMapExp(){

        List<String> stringList = Stream.of("a", "b", "c").map(s -> s.toUpperCase()).collect(Collectors.toList());
        String toString = Arrays.toString(stringList.toArray());
        System.out.println(toString);
    }

    // 转化为 list 操作
    public static void toListExp(){

        List<String> list = Stream.of("a", "b", "c").collect(Collectors.toList());
        System.out.println(list.size());

    }

    public static void streamDemo1(){

        List<Student> studentList = StudentFactory.createStudentList();

        long count = studentList.stream().count();
        // 惰性操作还是及早求值
        long count1 = studentList.stream().filter(t -> "女".equals(t.sex)).count();

        System.out.println("总个数: " + count);
        System.out.println("女总个数: " + count1);

    }

}
