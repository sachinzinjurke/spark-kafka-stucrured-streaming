package com.example.spark.streaming.pojo;

import java.io.Serializable;

public class Person implements Serializable {

    private String name;
    private  String surname;

    public Person(){

    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }
}
