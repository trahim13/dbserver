package org.trahim.row;

public class Person {

    public String name;
    public int age;
    public String address;
    public String carPlateNumber;
    public String description;

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", address='" + address + '\'' +
                ", carPlateNumber='" + carPlateNumber + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
