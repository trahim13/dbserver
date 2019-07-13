import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.trahim.dbserver.DBFactory;
import org.trahim.dbserver.DBGeneric;
import org.trahim.dbserver.DBGenericServer;
import org.trahim.exceptions.DBException;
import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.general.GenericIndex;
import org.trahim.util.DebugRowInfo;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class DBGenericBasicTests {
    private final String dbFileName = "testGeneric.db";
    private final static String DOG_SCHEMA = "{\n" +
            "  \"version\":\"0.1\",\n" +
            "  \"fields\":[\n" +
            "    {\"fieldName\": \"pname\", \"fieldType\":\"String\"},\n" +
            "    {\"fieldName\": \"age\",\"fieldType\": \"int\" },\n" +
            "    {\"fieldName\": \"owner\", \"fieldType\":\"String\"}\n" +
            "  ],\n" +
            "\n" +
            "  \"indexBy\": \"pname\"\n" +
            "}";

    @Before
    public void setup() {
        File file = new File(dbFileName);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testAdd() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {

            Dog dog = new Dog("JON", 3, "3");
            db.beginTransaction();
            db.add(dog);
            db.commit();
            Assert.assertEquals(GenericIndex.getInstance().getTotalNumberOfRows(), 1);
        } catch (IOException e) {
            Assert.fail();

        }
    }


    @Test
    public void testRead() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {

            Dog dog = new Dog("JON", 3, "3");

            db.beginTransaction();
            db.add(dog);
            db.commit();
            Assert.assertEquals(GenericIndex.getInstance().getTotalNumberOfRows(), 1);
            Dog result = (Dog) db.read(0);
            Assert.assertTrue(result.pname.equals("JON"));
            Assert.assertTrue(result.age == 3);
            Assert.assertTrue(result.owner.equals("3"));
        } catch (IOException e) {
            Assert.fail();

        }
    }

    @Test
    public void testDelete() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON", 3, "3");

            db.beginTransaction();
            db.add(dog);
            db.commit();

            Assert.assertEquals(GenericIndex.getInstance().getTotalNumberOfRows(), 1);

            db.beginTransaction();
            db.delete(0);
            db.commit();
            Assert.assertEquals(GenericIndex.getInstance().getTotalNumberOfRows(), 0);

        } catch (IOException e) {
            Assert.fail();

        }
    }

    @Test
    public void updateByName() throws DuplicateNameException, DBException, NoSuchFieldException, IllegalAccessException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON", 3, "3");
            db.beginTransaction();
            db.add(dog);
            db.commit();

            Dog dog2 = new Dog("JON", 4, "3");


            db.beginTransaction();
            db.update("JON", dog2);
            db.commit();

            Dog result = (Dog) db.read(1);

            Assert.assertEquals("JON", result.pname);
            Assert.assertTrue(result.age == 4);

        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test
    public void updateByRowNumber() throws DuplicateNameException, DBException, NoSuchFieldException, IllegalAccessException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON", 3, "3");
            db.beginTransaction();
            db.add(dog);
            db.commit();

            Dog dog2 = new Dog("JON", 4, "3");

            db.beginTransaction();
            db.update(0, dog2);
            db.commit();

            Dog result = (Dog) db.read(1);
            Assert.assertEquals("JON", result.pname);
            Assert.assertTrue(result.age == 4);

        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test
    public void teatSearch() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON_1", 3, "3");
            Dog dog2 = new Dog("JON_2", 4, "3");

            db.beginTransaction();
            db.add(dog);
            db.add(dog2);
            db.commit();

            Dog result = (Dog) db.search("JON_2");
            Assert.assertEquals("JON_2", result.pname);
            Assert.assertEquals(4, result.age);
            Assert.assertEquals("3", result.owner);

        } catch (IOException ioe) {
            Assert.fail();

        }


    }

    @Test
    public void teatSearchWithLeveishtein() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON1", 3, "3");
            Dog dog2 = new Dog("JON_2", 4, "3");

            db.beginTransaction();
            db.add(dog);
            db.add(dog2);
            db.commit();

            List<Object> result = db.searchWithLeveinshtein("JON1", 0);
            Assert.assertEquals(result.size(), 1);
            Assert.assertEquals("JON1", ((Dog) result.get(0)).pname);
            Assert.assertEquals(3, ((Dog) result.get(0)).age);
            Assert.assertEquals("3", ((Dog) result.get(0)).owner);


        } catch (IOException ioe) {
            Assert.fail();

        }
    }


    @Test
    public void teatSearchWith_tolerance_1_Leveishtein() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON_1", 3, "3");
            Dog dog2 = new Dog("JON_2", 4, "3");

            db.beginTransaction();
            db.add(dog);
            db.add(dog2);
            db.commit();

            List<Object> result = db.searchWithLeveinshtein("JON_2", 1);
            Assert.assertEquals(result.size(), 2);

        } catch (IOException ioe) {
            Assert.fail();

        }
    }

    @Test
    public void testWithRegexp() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON_1", 3, "3");
            Dog dog2 = new Dog("JON_2", 4, "3");

            db.beginTransaction();
            db.add(dog);
            db.add(dog2);
            db.commit();

            List<Object> result = db.searchWithRegexp("JON.*");

            Assert.assertEquals(result.size(), 2);

        } catch (IOException ioe) {
            Assert.fail();

        }
    }


    @Test
    public void transactionTest_COMMIT() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON_1", 3, "3");

            db.beginTransaction();
            db.add(dog);
            db.commit();

            List<Object> result = db.searchWithRegexp("JON.*");
            Assert.assertEquals(result.size(), 1);

            Dog findDog = (Dog) result.get(0);
            Assert.assertEquals(findDog.pname, "JON_1");

        } catch (IOException ioe) {
            Assert.fail();

        }
    }


    @Test
    public void transactionTest_ROLLBACK() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON_1", 3, "3");

            db.beginTransaction();
            db.add(dog);
            db.rollback();

            List<Object> result = db.searchWithRegexp("JON.*");
            Assert.assertEquals(result.size(), 0);

            List<DebugRowInfo> infos = ((DBGenericServer) db).listAllRowsWithDebug();
            Assert.assertEquals(infos.size(), 1);

            DebugRowInfo dri = infos.get(0);
            Assert.assertEquals(dri.isTemporary(), false);
            Assert.assertEquals(dri.isDeleted(), true);

        } catch (IOException ioe) {
            Assert.fail();

        }
    }


    @Test
    public void transactionTest_COMMIT_with_multiple_begin() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON_1", 3, "3");

            db.beginTransaction();
            db.add(dog);
            db.beginTransaction();
            db.commit();

            List<Object> result = db.searchWithRegexp("JON.*");
            Assert.assertEquals(result.size(), 1);

            Dog searchDog = (Dog) result.get(0);
            Assert.assertEquals(searchDog.pname, "JON_1");

        } catch (IOException ioe) {
            Assert.fail();

        }
    }

    @Test
    public void transactionTest_ROLLBACk_with_multiple_begin() throws DuplicateNameException, DBException {
        try (DBGeneric db = DBFactory.getGenericDB(dbFileName, DOG_SCHEMA, Dog.class)) {
            Dog dog = new Dog("JON_1", 3, "3");
            Dog dog2 = new Dog("JON_2", 4, "3");

            db.beginTransaction();
            db.add(dog);
            db.beginTransaction();
            db.add(dog2);
            db.rollback();

            List<Object> result = db.searchWithRegexp("JON.*");
            Assert.assertEquals(result.size(), 0);

            List<DebugRowInfo> infos = db.listAllRowsWithDebug();
            Assert.assertEquals(infos.size(), 2);

            DebugRowInfo dri = infos.get(0);
            Assert.assertEquals(dri.isTemporary(), false);
            Assert.assertEquals(dri.isDeleted(), true);

        } catch (IOException ioe) {
            Assert.fail();

        }
    }

}
