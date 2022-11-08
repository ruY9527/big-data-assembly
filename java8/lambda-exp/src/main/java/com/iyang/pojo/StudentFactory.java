package com.iyang.pojo;

import java.util.ArrayList;
import java.util.List;

/***
 * @author: baoyang
 * @data: 2022/11/8
 * @desc:
 ***/
public class StudentFactory {

    public static List<Student> createStudentList(){
        List<Student> studentList = new ArrayList<>();

        Student student1 = new Student("zhangsan", "男", 25);
        Student student2 = new Student("lisi", "男", 25);
        Student student3 = new Student("maliu", "女", 25);

        studentList.add(student1);
        studentList.add(student2);
        studentList.add(student3);

        return studentList;
    }

}
