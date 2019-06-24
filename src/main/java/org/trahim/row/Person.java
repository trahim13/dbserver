package org.trahim.row;

import com.google.gson.JsonObject;

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

    public String toJSON() {
        JsonObject json = new JsonObject();
        json.addProperty("name", this.name);
        json.addProperty("age", this.age);
        json.addProperty("address", this.address);
        json.addProperty("carPlateNumber", this.carPlateNumber);
        json.addProperty("description", this.description);

        return json.toString();
    }
}
