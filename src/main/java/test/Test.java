package test;

import org.trahim.dbserver.DB;
import org.trahim.dbserver.DBServer;
import org.trahim.row.FileHandler;
import org.trahim.row.Index;
import org.trahim.row.Person;

import java.io.FileNotFoundException;
import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {

        try {

            final String dbFile = "DbServer.db";
            DB db = new DBServer(dbFile);
            db.add("1kmnmn", 2, "3", "4", "5");
            db.close();

            db = new DBServer(dbFile);
            Person person = db.read(0);


            System.out.println("Total number of rows in database: " + Index.getInstance().getTotalNumberOfRows());
            System.out.println(person);


            db.delete(0);
            System.out.println("Total number of rows in database: " + Index.getInstance().getTotalNumberOfRows());

            db.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
