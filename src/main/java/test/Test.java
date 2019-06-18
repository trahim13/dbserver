package test;

import org.trahim.dbserver.DB;
import org.trahim.dbserver.DBServer;
import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.Index;
import org.trahim.row.Person;
import org.trahim.util.DebugRowInfo;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

class Test {
    private final static String dbFile = "DbServer.db";

    public static void main(String[] args) throws DuplicateNameException {

        new Test().performTest();


    }

    private void performTest() throws DuplicateNameException {
        try {
            fragmentDatabase();
//            listAllRecords();
//            defragmentDB();
//            System.out.println("-=After defragmentation=-");
//            listAllRecords();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void defragmentDB() throws IOException, DuplicateNameException {
        try (DBServer db = new DBServer(dbFile)) {
            db.defragmentDatabase();
        } catch (IOException e) {
            throw e;
        }
    }

    private void testSearhWhithRegex() throws IOException {
        try (DB db = new DBServer(dbFile)) {
            List<Person> result = db.searchWithRegexp("Pers.*");
            System.out.println("-=Search with Regexp=-");
            for (Person p :
                    result) {
                System.out.println(p);
            }
        } catch (IOException e) {
            throw e;
        }
    }

    public void testLevenshtein() throws IOException {
        try (DB db = new DBServer(dbFile)) {
            List<Person> result = db.searchWithLeveinshtein("Person 4", 0);
            System.out.println("-=Search with Levenshtein=-");
            for (Person p :
                    result) {
                System.out.println(p);
            }
        } catch (IOException e) {
            throw e;
        }
    }

    public void testSearch() throws IOException {
        try (DB db = new DBServer(dbFile)) {
            Person result = db.search("Person 4");
            System.out.println("Person " + result);
        } catch (IOException e) {
            throw e;
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

    private void fillDB() throws IOException, DuplicateNameException {
        try (DB db = new DBServer(dbFile)) {
            for (int i = 0; i < 10; i++) {
                Person p0 = new Person("Person " + i, 3, "3", "4", "5");
                db.add(p0);
            }

        } catch (IOException ioe) {
            throw ioe;
        }
    }

    public void fragmentDatabase() throws IOException, DuplicateNameException {
        try (DB db = new DBServer(dbFile)) {
            for (int i :
                    IntStream.range(0, 100).toArray()) {

                Person p0 = new Person("Person " + i, 3, "3", "4", "5");
                db.add(p0);
            }

            for (int i : IntStream.range(0, 100).toArray()) {
                if (i % 2 == 0) {
                    db.update("Person " + i, new Person("Person " + i + "_updated", 3, "3", "4", "5"));
                }
            }
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
