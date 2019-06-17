package test;

import org.trahim.dbserver.DB;
import org.trahim.dbserver.DBServer;
import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.Index;
import org.trahim.row.Person;
import org.trahim.util.DebugRowInfo;

import java.io.IOException;
import java.util.List;

public class Test {
    final static String dbFile = "DbServer.db";

    public static void main(String[] args) throws IOException, DuplicateNameException {

        new Test().performTest();


    }

    private void performTest() throws DuplicateNameException {
        try {
            fillDB(10);
            delete(0);
            delete(2);
            delete(5);

            listAllRecords();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    void listAllRecords() throws IOException {


        try (DBServer db = new DBServer(dbFile)) {
            List<DebugRowInfo> result = db.listAllRowsWithDebug();
            System.out.println("Total row number : " + Index.getInstance().getTotalNumberOfRows());
            for (DebugRowInfo dri :
                    result) {
                prittyPrintRow(dri);
            }
        } catch (IOException ioe) {
            throw ioe;
        }

    }

    private void prittyPrintRow(DebugRowInfo dri) {
        Person person = dri.getPerson();
        boolean isDeleted = dri.isDeleted();

        String debugChar = isDeleted ? "-" : "+";

        String s = String.format(" %s, name: %s, age: %d, description: %s, carPlate: %s", debugChar,
                person.name,
                person.age,
                person.description,
                person.carPlateNumber
        );
        System.out.println(s);
    }

    public void fillDB(int number) throws IOException, DuplicateNameException {
        try (DB db = new DBServer(dbFile)) {
            for (int i = 0; i < number; i++) {
                Person p0 = new Person("Person " + i, 3, "3", "4", "5");
                db.add(p0);
            }

        } catch (IOException ioe) {
            throw ioe;
        }


    }

    public void delete(int rowNumber) throws IOException {
        try (DB db = new DBServer(dbFile)) {
            db.delete(rowNumber);
        } catch (IOException ioe) {
            throw ioe;
        }

    }
}
