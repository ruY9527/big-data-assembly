package com.iyang.lambda.sorts;

import com.iyang.pojo.Student;
import com.iyang.pojo.StudentFactory;

import java.util.*;
import java.util.stream.Collectors;

/***
 * @author: baoyang
 * @data: 2022/11/8
 * @desc:
 ***/
public class SortCollectsExp {


    public static void main(String[] args) {

        // Collectors 收集器
        //strCollector();
        //groupCollector();
        groupCollectorManyFields();

    }

    public static void groupCollectorManyFields(){

        List<Student> studentList = StudentFactory.createStudentList();
        Map<String, List<Integer>> collect = studentList.stream().collect(Collectors.groupingBy(Student::getName,
                // 收集出对应想要的字段集合, 私用 mapping 来进行一层映射
                Collectors.mapping(Student::getAge, Collectors.toList())));

        // (k1,k2) -> k1 , 避免出现相同key的时候,会引发出错误
        Map<String, Integer> integerMap = studentList.stream()
                .collect(Collectors.toMap(
                        key -> key.name,
                        value -> value.age,
                        (k1,k2) -> k1));

        System.out.println(integerMap);
        System.out.println(collect);

    }

    public static void groupCollector(){

        List<Student> studentList = StudentFactory.createStudentList();
        Map<String, Long> stringLongMap = studentList.stream().collect(Collectors.groupingBy(s -> s.name, Collectors.counting()));
        System.out.println(stringLongMap);

    }

    public static void strCollector(){

        List<Student> studentList = StudentFactory.createStudentList();
        String resultStr = studentList.stream().map(Student::getName).collect(Collectors.joining(",", "[", "]"));
        System.out.println(resultStr);

    }

    public static void sorted(){

        Set<Integer> set = new HashSet<>(Arrays.asList(4,3,1,2));
        List<Integer> collect = set.stream().sorted().collect(Collectors.toList());
        System.out.println(Arrays.toString(collect.toArray()));

    }

}
