package com.iyang.pojo;

/***
 * @author: baoyang
 * @data: 2022/11/8
 * @desc:
 ***/
public class Track {

    public String name;
    public Integer numbers;

    public Track() {
    }

    public Track(String name, Integer numbers) {
        this.name = name;
        this.numbers = numbers;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getNumbers() {
        return numbers;
    }

    public void setNumbers(Integer numbers) {
        this.numbers = numbers;
    }

    @Override
    public String toString() {
        return "Track{" +
                "name='" + name + '\'' +
                ", numbers=" + numbers +
                '}';
    }
}
