package com.boco.bomc.spark.cases;

import java.io.Serializable;

public class User implements Serializable {
    /**
     * 用户ID 性别 年龄 注册时间      角色    地区
     * 数据格式：1      M   23   2000-04-12  ROLE001 REG001
     */

    private String uid;
    private String gender;
    private int age;
    private String registerDate;
    private String roleName;
    private String regionName;

    public User(String uid, String gender, int age, String registerDate, String roleName, String regionName) {
        this.uid = uid;
        this.gender = gender;
        this.age = age;
        this.registerDate = registerDate;
        this.roleName = roleName;
        this.regionName = regionName;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getRegisterDate() {
        return registerDate;
    }

    public void setRegisterDate(String registerDate) {
        this.registerDate = registerDate;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }
}
