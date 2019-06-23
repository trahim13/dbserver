package test;

import org.trahim.dbserver.DB;
import org.trahim.dbserver.DBServer;
import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.Index;
import org.trahim.row.Person;
import org.trahim.transaction.ITransaction;
import org.trahim.util.DebugRowInfo;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

class Test {
    private final static String dbFile = "DbServer.db";

    public static void main(String[] args) throws DuplicateNameException {

        new Test().performTest();


    }

    private void performTest() throws DuplicateNameException {
        try {
            deleteDataBase();
//            fragmentDatabase();
//            listAllRecords();
//            defragmentDB();
//            System.out.println("-=After defragmentation=-");
//            listAllRecords();
            addPersonWhithTransaction();
            listAllRecords();
//            doMultipleTreadsTest();
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
        boolean isTemporary = dri.isTemporary();
        boolean isDeleted = dri.isDeleted();

        String deletedChar = isDeleted ? "-" : "+";
        String temporaryChar = isTemporary ? "temporary" : "final";

        String s = String.format("%s %s, name: %s, age: %d, description: %s, carPlate: %s",
                temporaryChar,
                deletedChar,
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

    private void doMultipleTreadsTest() throws IOException {
        CountDownLatch countDownLatch = new CountDownLatch(3);

        try (DB db = new DBServer(dbFile)) {
            Runnable runnableAdd = () -> {
                while (true) {
                    int i = new Random().nextInt(4000);
                    Person p0 = new Person("Person " + i, 3, "3", "4", "5");
                    try {
                        db.add(p0);
                    } catch (IOException | DuplicateNameException e) {
                        e.printStackTrace();
                    }

                }
            };
            Runnable runnableUpdate = () -> {
                while (true) {
                    int i = new Random().nextInt(4000);
                    Person p0 = new Person("Person " + i + "__updated", 3, "3", "4", "5");
                    try {
                        db.update("Person " + 1, p0);
                    } catch (IOException | DuplicateNameException e) {
                        e.printStackTrace();
                    }

                }
            };
            Runnable runnableListAll = () -> {
                while (true) {
                    try {
                        db.listAllRowsWithDebug();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            };

            ExecutorService executorService = Executors.newFixedThreadPool(3);
            executorService.submit(runnableListAll);
            executorService.submit(runnableUpdate);
            executorService.submit(runnableAdd);

            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


    }

    public void addPersonWhithTransaction() throws IOException, DuplicateNameException {
        try (DB db = new DBServer(dbFile)) {
            ITransaction transaction = db.beginTransaction();
            Person p0 = new Person("Person T", 3, "3", "4", "5c");
            db.add(p0);
//            db.commit(); // rollback();
            db.rollback();

        }
    }


    private void deleteDataBase() {
        File file = new File(dbFile);
        if (file.exists()) {
            file.delete();
        }
    }
}
