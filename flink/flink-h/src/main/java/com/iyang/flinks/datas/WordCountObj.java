package com.iyang.flinks.datas;

/***
 * @author: baoyang
 * @data: 2022/10/31
 * @desc:
 ***/
public class WordCountObj {

    private String word;
    private int count;

    public WordCountObj() {
    }

    public WordCountObj(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCountObj{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
