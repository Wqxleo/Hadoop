package com.wquanxiu;

import java.util.Map;

/**
 * Created by wangquanxiu at 2019/4/16 20:00
 */
public class Student {

    private String id;
    private String name;
    private String classin;
    private String gender;
    private String birthday;
    private Map<String,String> scores;
    private String address;

    public Student(String id, String name, String classin, String gender, String birthday, Map<String,String> scores, String address) {
        this.id = id;
        this.name = name;
        this.classin = classin;
        this.gender = gender;
        this.birthday = birthday;
        this.scores = scores;
        this.address = address;
    }

    public Student(){

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getname() {
        return name;
    }

    public void setname(String name) {
        this.name = name;
    }

    public String getclassin() {
        return classin;
    }

    public void setclassin(String classin) {
        this.classin = classin;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getbirthday() {
        return birthday;
    }

    public void setbirthday(String birthday) {
        this.birthday = birthday;
    }

    public Map<String,String> getscores() {
        return scores;
    }

    public void setscores(Map<String,String> scores) {
        this.scores = scores;
    }

    public String getaddress() {
        return address;
    }

    public void setaddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        String studentinfo = "Student{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", classin='" + classin + '\'' +
                ", gender='" + gender + '\'' +
                ", birthday='" + birthday + '\'' +
                ", scores:'";

                for(Map.Entry<String,String> entry: scores.entrySet()){
                    studentinfo+=(entry.getKey()+"='"+entry.getValue()+"' ");
                }

                studentinfo+=(", address='" + address + '\'' +
                '}');

        return studentinfo;
    }
}