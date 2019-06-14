import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.trahim.dbserver.DB;
import org.trahim.dbserver.DBServer;
import org.trahim.row.Index;
import org.trahim.row.Person;

import java.io.File;
import java.io.IOException;

public class DBBasicTests {

    private DB db;
    private String dbFileName = "test.db";

    @Before
    public void setup() throws IOException {
        File file = new File(dbFileName);
        if (file.exists()) {
            file.delete();
        }
        this.db = new DBServer(dbFileName);
    }

    @Test
    public void testAdd() {
        try {
            this.db.add("1kmnmn", 2, "3", "4", "5");
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
        } catch (IOException e) {
            Assert.fail();

        }
    }


    @Test
    public void testRead() {
        try {
            this.db.add("1kmnmn", 2, "3", "4", "5");
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
            Person person = db.read(0);
            Assert.assertEquals(person.name, "1kmnmn");
            Assert.assertEquals(person.age, 2);
            Assert.assertEquals(person.address, "3");
            Assert.assertEquals(person.carPlateNumber, "4");
            Assert.assertEquals(person.description, "5");
        } catch (IOException e) {
            Assert.fail();

        }
    }

    @Test
    public void testDelete() {
        try {
            this.db.add("1kmnmn", 2, "3", "4", "5");
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
            this.db.delete(0);
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 0);

        } catch (IOException e) {
            Assert.fail();

        }
    }

    @After
    public void after() {
        try {
            this.db.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
