package test;

import org.trahim.row.FileHandler;
import org.trahim.row.Person;

import java.io.FileNotFoundException;
import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {

        try {
            FileHandler fileHandler = new FileHandler("test2.db");
            fileHandler.add("1", 2, "3", "4", "5");
            fileHandler.close();

            fileHandler = new FileHandler("test.db");
            Person person = fileHandler.readRow(0);
            fileHandler.close();

            System.out.println(person);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
