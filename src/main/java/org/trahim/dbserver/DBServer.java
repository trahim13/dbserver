package org.trahim.dbserver;

import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.FileHandler;
import org.trahim.row.Index;
import org.trahim.row.Person;
import org.trahim.transaction.ITransaction;
import org.trahim.transaction.Transaction;
import org.trahim.util.DebugRowInfo;
import org.trahim.util.OperationUnit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DBServer implements DB {

    private FileHandler fileHandler;

    private Map<Long, ITransaction> transactions;

    private final Logger LOGGER = Logger.getLogger("DBServer");
    private final static String LOG_FILE_NAME = "config.properties";
    private final static String LOG_LEVEL = "LOG_LEVEL";

    public DBServer(final String dbFileName) throws IOException {
        this.fileHandler = new FileHandler(dbFileName);
        this.transactions = new LinkedHashMap<>();
        this.initialise();
    }


    private void initialise() throws IOException {

        this.fileHandler.initialise();

        Properties properties = new Properties();
        properties.load(new FileInputStream((LOG_FILE_NAME)));
        boolean hasLogLevel = properties.containsKey(LOG_LEVEL);
        if (!hasLogLevel) {
            LOGGER.setLevel(Level.SEVERE);

        } else {
            String logLevel = (String) properties.get(LOG_LEVEL);

            if (logLevel.equalsIgnoreCase("SEVERE")) {
                LOGGER.setLevel(Level.SEVERE);
            } else if (logLevel.equalsIgnoreCase("INFO")) {
                LOGGER.setLevel(Level.INFO);
            } else if (logLevel.equalsIgnoreCase("DEBUG")) {
                LOGGER.setLevel(Level.ALL);

            }
        }


        this.fileHandler.loadAllDataToIndex();

    }

    @Override
    public void close() throws IOException {
        LOGGER.info("[" + this.getClass().getName() + "]" + " Closing DB server");
        Index.getInstance().clear();
        this.fileHandler.close();

    }


    @Override
    public void add(Person person) throws IOException, DuplicateNameException {
        logInfoPerson(person);

        OperationUnit ou = this.fileHandler.add(
                person.name,
                person.age,
                person.address,
                person.carPlateNumber,
                person.description);

        this.getTransaction().registerAdd(ou.addedRowPosition);

    }

    @Override
    public void delete(long rowNumber) throws IOException {
        if (rowNumber < 0) {
            LOGGER.info("[" + this.getClass().getName() + "]" + " Row number is less then 0:  " + rowNumber);
            throw new IOException("Row number is less then 0");
        }
        LOGGER.info("[" + this.getClass().getName() + "]" + " Deleting person. Row number: " + rowNumber);

        OperationUnit ou = this.fileHandler.deleteRow(rowNumber);
        this.getTransaction().registerDelete(ou.deletedRowPosition);

    }

    @Override
    public Person read(long rowNumber) throws IOException {
        LOGGER.info("[" + this.getClass().getName() + "]. " + "Reading row. Row number: " + rowNumber);
        Person person = this.fileHandler.readRow(rowNumber);

        logInfoPerson(person);
        return person;
    }

    @Override
    public void update(long rowNumber, final Person person) throws IOException, DuplicateNameException {
        LOGGER.info("[" + this.getClass().getName() + "]. " + "Updating person. Row number: " + rowNumber + ". Person " + person);
        OperationUnit ou = this.fileHandler.updateRow(rowNumber,
                person.name, person.age, person.address, person.carPlateNumber, person.description);
        ITransaction transaction = this.getTransaction();
        transaction.registerDelete(ou.deletedRowPosition);
        transaction.registerAdd(ou.addedRowPosition);
    }

    @Override
    public void update(String name, Person person) throws IOException, DuplicateNameException {
        LOGGER.info("Updating person. Name: " + name + ". Person " + person);
        OperationUnit ou = this.fileHandler.update(name,
                person.name, person.age, person.address, person.carPlateNumber, person.description);
        ITransaction transaction = this.getTransaction();
        transaction.registerDelete(ou.deletedRowPosition);
        transaction.registerAdd(ou.addedRowPosition);
    }

    @Override
    public Person search(String name) throws IOException {
        LOGGER.info("Searching person: " + name);
        Person person = this.fileHandler.search(name);
        this.logInfoPerson(person);
        return person;
    }

    @Override
    public List<DebugRowInfo> listAllRowsWithDebug() throws IOException {
        return this.fileHandler.loadAllDataFromFile();
    }

    @Override
    public List<Person> searchWithLeveinshtein(String name, int tolerance) throws IOException {
        LOGGER.info("[" + this.getClass().getName() + "]. " +"Searching with Levenshtein. Name: " + name + ". tolerance: " + tolerance);

        List<Person> persons = this.fileHandler.searchWithLeveinshtein(name, tolerance);

        this.logInfoListPerson(persons);

        return persons;
    }

    @Override
    public List<Person> searchWithRegexp(String regexp) throws IOException {
        LOGGER.info("[" + this.getClass().getName() + "]. " +"Searching with Regex. Regex: " + regexp);
        List<Person> persons = this.fileHandler.searchWithRegex(regexp);
        this.logInfoListPerson(persons);

        return persons;
    }

    public void defragmentDatabase() throws IOException, DuplicateNameException {
        LOGGER.info("[" + this.getClass().getName() + "]. "+ "Defragmenting Database");

        //создали временный файл
        File tmpFile = File.createTempFile("defrag", "dat");
        Index.getInstance().clear();

        //открыли временный файл и записали туда неудалённые значения из базы данных
        FileHandler defragFileHandler = new FileHandler(new RandomAccessFile(tmpFile, "rw"), tmpFile.getName());
        List<DebugRowInfo> debugRowInfos = this.fileHandler.loadAllDataFromFile();
        for (DebugRowInfo dri : debugRowInfos) {
            if (dri.isDeleted() || dri.isTemporary()) {
                continue;

            }
            Person p = dri.getPerson();

            defragFileHandler.add(p.name, p.age, p.address, p.carPlateNumber, p.description);
        }
        //удалили старый файл DB
        boolean wasDeleted = this.fileHandler.deleteFile();
        if (!wasDeleted) {
            tmpFile.delete();
            LOGGER.severe("[" + this.getClass().getName() + "]. "+"Database file cannot be deleted during the defragmentation");
            this.initialise();
            throw new IOException("[" + this.getClass().getName() + "]. "+"DB cannot be deleted. Check the logs.");
        }
        this.fileHandler.close();
        String oldDataBaseName = this.fileHandler.getDBName();
        //копировали на место старого файла новый с неудалённвми данными
        Files.copy(tmpFile.toPath(), FileSystems.getDefault().getPath("", oldDataBaseName),
                StandardCopyOption.REPLACE_EXISTING);
        //закрыли временный файл
        defragFileHandler.close();
        // подсунули текущему filehandlery новый файл
        this.fileHandler = new FileHandler(oldDataBaseName);
        Index.getInstance().clear();
        //переинициализировали index
        this.initialise();
        LOGGER.info("[" + this.getClass().getName() + "]. "+ "Database file has been defragmented");
    }

    private ITransaction getTransaction() {
        long threadId = Thread.currentThread().getId();
        LOGGER.info("[ " + this.getClass().getName() + " ]. " + "Get transaction with id " + threadId);
        return this.transactions.getOrDefault(threadId, null);
    }

    @Override
    public ITransaction beginTransaction() {
        long threadId = Thread.currentThread().getId();

        if (this.transactions.containsKey(threadId)) {
            return this.transactions.get(threadId);
        }
        ITransaction transaction = new Transaction();
        this.transactions.put(threadId, transaction);
        return transaction;
    }

    @Override
    public void commit() throws IOException {
        ITransaction transaction = this.getTransaction();

        if (transaction == null) {
            LOGGER.info("[" + this.getClass().getName() + "]. " + " Transaction is not found!");

            return;
        }

        this.fileHandler.commit(transaction.getNewRows(), transaction.getDeletedRows());
        this.transactions.remove(Thread.currentThread().getId());
        this.transactions.clear();
        LOGGER.info("[" + this.getClass().getName() + "]. " + " Commit DONE (" + transaction.getUid() + " )");


    }

    @Override
    public void rollback() throws IOException {
        ITransaction transaction = this.getTransaction();

        if (transaction == null) {
            return;
        }

        this.fileHandler.rollback(transaction.getNewRows(), transaction.getDeletedRows());
        this.transactions.remove(Thread.currentThread().getId());
        this.transactions.clear();

        LOGGER.info("[" + this.getClass().getName() + "]. " + " Rollback DONE. (" + transaction.getUid()+" )");
    }

    @Override
    public long getTotalRecordNumber() {
        return Index.getInstance().getTotalNumberOfRows();
    }


    private void logInfoPerson(final Person person) {
        LOGGER.info("[" + this.getClass().getName() + "]. " + "Read person: " + person);
    }
    private void logInfoListPerson(final List<Person> persons) {

        StringBuilder sb = new StringBuilder(300);

        for (Person person : persons){
            sb.append(person.toString());
            sb.append(System.getProperty("line.separator"));
        }

        LOGGER.info("[" + this.getClass().getName() + "]. " + "Read persons: " + sb);

    }
}