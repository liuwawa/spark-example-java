package com.boco.bomc.spark.cases;

import java.io.Serializable;

public class Person implements Serializable {

    private String id;
    private String gender;
    private int height;

    public Person(String id, String gender, int height) {
        this.id = id;
        this.gender = gender;
        this.height = height;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getHeight() {
        return height;
    }

    public String getGender() {
        return gender;
    }

    public String getId() {
        return id;
    }
}
