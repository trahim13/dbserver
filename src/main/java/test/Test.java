package test;

import org.trahim.dbserver.DB;
import org.trahim.dbserver.DBServer;
import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.FileHandler;
import org.trahim.row.Index;
import org.trahim.row.Person;

import java.io.FileNotFoundException;
import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException, DuplicateNameException {

        try {

            final String dbFile = "DbServer.db";
            DB db = new DBServer(dbFile);

            Person p0 = new Person("1kmnmn", 3, "3", "4", "5");


            db.add(p0);

            System.out.println("Total number of rows in database: " + Index.getInstance().getTotalNumberOfRows());

            Person p1 = new Person("2kmnmn", 3, "3", "4", "5");


            db.update("1kmnmn", p1);

            Person updatedPerson = db.read(0);
            System.out.println(updatedPerson);
            System.out.println("Total number of rows in database: " + Index.getInstance().getTotalNumberOfRows());
            db.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
