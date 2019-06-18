import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.trahim.dbserver.DB;
import org.trahim.dbserver.DBServer;
import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.Index;
import org.trahim.row.Person;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class DBBasicTests {

    private final String dbFileName = "test.db";

    @Before
    public void setup() {
        File file = new File(dbFileName);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testAdd() throws DuplicateNameException {
        try (DB db = new DBServer(dbFileName)) {
            Person p0 = new Person("1kmnmn", 3, "3", "4", "5");
            db.add(p0);
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
        } catch (IOException e) {
            Assert.fail();

        }
    }


    @Test
    public void testRead() throws DuplicateNameException {
        try (DB db = new DBServer(dbFileName)) {

            Person p0 = new Person("1kmnmn", 3, "3", "4", "5");
            db.add(p0);
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
            Person person = db.read(0);
            Assert.assertEquals(person.name, "1kmnmn");
            Assert.assertEquals(person.age, 3);
            Assert.assertEquals(person.address, "3");
            Assert.assertEquals(person.carPlateNumber, "4");
            Assert.assertEquals(person.description, "5");
        } catch (IOException e) {
            Assert.fail();

        }
    }

    @Test
    public void testDelete() throws DuplicateNameException {
        try (DB db = new DBServer(dbFileName)) {
            Person p0 = new Person("1kmnmn", 3, "4", "5", "6");
            db.add(p0);
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
            db.delete(0);
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 0);

        } catch (IOException e) {
            Assert.fail();

        }
    }

    @Test
    public void updateByName() throws DuplicateNameException {
        try (DB db = new DBServer(dbFileName)) {
            Person p0 = new Person("Test 0", 3, "4", "5", "6");
            db.add(p0);

            Person p1 = new Person("Test 1", 3, "4", "5", "6");
            db.update("Test 0", p1);
            Person result = db.read(0);
            Assert.assertEquals("Test 1", result.name);
        } catch (IOException e) {
            Assert.fail();
        }
    }
    @Test
    public void updateByRowNumber() throws DuplicateNameException {
        try (DB db = new DBServer(dbFileName)) {
            Person p0 = new Person("Test 0", 3, "4", "5", "6");
            db.add(p0);

            Person p1 = new Person("Test 1", 3, "4", "5", "6");
            db.update(0, p1);
            Person result = db.read(0);
            Assert.assertEquals("Test 1", result.name);
        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test
    public void teatSearch() throws DuplicateNameException {
        try (DB db = new DBServer(dbFileName)) {
            Person p0 = new Person("Test 0", 3, "4", "5", "6");
            Person p1 = new Person("Test 1", 55, "4", "5", "6");
            db.add(p0);
            db.add(p1);

            Person result = db.search("Test 1");
            Assert.assertEquals("Test 1", result.name);

        } catch (IOException ioe) {
            Assert.fail();

        }



    }

    @Test
    public void teatSearchWithLeveishtein() throws DuplicateNameException {
        try (DB db = new DBServer(dbFileName)) {
            Person p0 = new Person("Test 0", 3, "4", "5", "6");
            Person p1 = new Person("Test 1", 55, "4", "5", "6");
            db.add(p0);
            db.add(p1);

            List<Person> result = db.searchWithLeveinshtein("Test 1", 0);
            Assert.assertEquals(result.size(), 1);
            Assert.assertEquals(result.get(0).name, "Test 1");

        } catch (IOException ioe) {
            Assert.fail();

        }
    }


    @Test
    public void teatSearchWith_tolerance_1_Leveishtein() throws DuplicateNameException {
        try (DB db = new DBServer(dbFileName)) {
            Person p0 = new Person("Test 0", 3, "4", "5", "6");
            Person p1 = new Person("Test 1", 55, "4", "5", "6");
            db.add(p0);
            db.add(p1);

            List<Person> result = db.searchWithLeveinshtein("Test 1", 1);
            Assert.assertEquals(result.size(), 2);

        } catch (IOException ioe) {
            Assert.fail();

        }
    }

    @Test
   public void testWithRegexp() throws DuplicateNameException {
        try (DB db = new DBServer(dbFileName)) {
            Person p0 = new Person("Test 0", 3, "4", "5", "6");
            Person p1 = new Person("Test 1", 55, "4", "5", "6");
            db.add(p0);
            db.add(p1);

            List<Person> result = db.searchWithRegexp("Tes.*");
            Assert.assertEquals(result.size(), 2);

        } catch (IOException ioe) {
            Assert.fail();

        }
    }

}
