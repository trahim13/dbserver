package org.trahim.row;

public class Person {

    public String name;
    public int age;
    public String address;
    public String carPlateNumber;
    public String description;

    public Person() {
    }

    public Person(final String name, final int age, final String address, final String carPlateNumber,final String description) {
        this.name = name;
        this.age = age;
        this.address = address;
        this.carPlateNumber = carPlateNumber;
        this.description = description;
    }

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
