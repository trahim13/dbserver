package org.trahim.dbserver;

import com.google.gson.Gson;
import org.trahim.exceptions.DBException;
import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.Person;
import org.trahim.row.general.Field;
import org.trahim.row.general.GenericFileHandler;
import org.trahim.row.general.GenericIndex;
import org.trahim.row.general.Schema;
import org.trahim.row.specific.FileHandler;
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

public final class DBGenericServer implements DBGeneric {

    private GenericFileHandler fileHandler;

    private Map<Long, ITransaction> transactions;

    private final Logger LOGGER = Logger.getLogger("DBServer");
    private final static String LOG_FILE_NAME = "config.properties";
    private final static String LOG_LEVEL = "LOG_LEVEL";

    private Schema schema;
    private Class zclass;

    public DBGenericServer(final String dbFileName,
                           final String schema,
                           final Class zclass) throws IOException, DBException {

        this.schema = this.readSchema(schema);
        this.zclass = zclass;


        this.fileHandler = new GenericFileHandler(dbFileName);
        this.fileHandler.setSchema(this.schema);
        this.fileHandler.setZClass(this.zclass);


        this.transactions = new LinkedHashMap<>();
        this.initialise();
    }

    private Schema readSchema(final String schema) {
        Gson gson = new Gson();
        Schema tmpSchema = gson.fromJson(schema, Schema.class);

        for (Field field : tmpSchema.fields) {
            LOGGER.info(field.toString());

        }

        return tmpSchema;
    }


    private void initialise() throws IOException, DBException {
        GenericIndex.getInstance().initialize(this.schema);

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


        this.fileHandler.loadAllDataToIndex(this.zclass);

    }

    @Override
    public void close() throws IOException {
        LOGGER.info("[" + this.getClass().getName() + "]" + " Closing DB server");
        GenericIndex.getInstance().clear();
        this.fileHandler.close();

    }


    @Override
    public void add(Object object) throws IOException, DuplicateNameException, DBException {
        logInfoObject(object);

        OperationUnit ou = this.fileHandler.add(
                object);

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
    public Object read(long rowNumber) throws IOException {
        LOGGER.info("[" + this.getClass().getName() + "]. " + "Reading row. Row number: " + rowNumber);
        Object object = this.fileHandler.readRow(rowNumber);

        logInfoObject(object);
        return object;
    }

    @Override
    public void update(long rowNumber, final Object object) throws IOException, DuplicateNameException, DBException, NoSuchFieldException, IllegalAccessException {
        LOGGER.info("[" + this.getClass().getName() + "]. " + "Updating object. Row number: " + rowNumber + ". Object " + object);
        OperationUnit ou = this.fileHandler.updateRow(rowNumber, object);
        ITransaction transaction = this.getTransaction();
        transaction.registerDelete(ou.deletedRowPosition);
        transaction.registerAdd(ou.addedRowPosition);
    }

    @Override
    public void update(String indexedFieldName, Object object) throws IOException, DuplicateNameException, DBException, NoSuchFieldException, IllegalAccessException {
        LOGGER.info("Updating person. Name: " + indexedFieldName + ". Object " + object);
        OperationUnit ou = this.fileHandler.update(indexedFieldName, object);
        ITransaction transaction = this.getTransaction();
        transaction.registerDelete(ou.deletedRowPosition);
        transaction.registerAdd(ou.addedRowPosition);
    }

    @Override
    public Object search(String indexedFieldName) throws IOException {
        LOGGER.info("Searching person: " + indexedFieldName);
        Object object = this.fileHandler.search(indexedFieldName);
        this.logInfoObject(object);
        return object;
    }

    @Override
    public List<DebugRowInfo> listAllRowsWithDebug() throws IOException {
        return this.fileHandler.loadAllDataFromFile(this.zclass);
    }

    @Override
    public List<Object> searchWithLeveinshtein(String indexedFieldName, int tolerance) throws IOException {
        LOGGER.info("[" + this.getClass().getName() + "]. " + "Searching with Levenshtein. Name: " + indexedFieldName + ". tolerance: " + tolerance);

        List<Object> result = this.fileHandler.searchWithLeveinshtein(indexedFieldName, tolerance);

        this.logInfoListObject(result);

        return result;
    }

    @Override
    public List<Object> searchWithRegexp(String regexp) throws IOException {
        LOGGER.info("[" + this.getClass().getName() + "]. " + "Searching with Regex. Regex: " + regexp);
        List<Object> result = this.fileHandler.searchWithRegex(regexp);
        this.logInfoListObject(result);

        return result;
    }

    public void defragmentDatabase() throws IOException, DuplicateNameException, DBException {
        LOGGER.info("[" + this.getClass().getName() + "]. " + "Defragmenting Database");

        //создали временный файл
        File tmpFile = File.createTempFile("defrag", "dat");
        GenericIndex.getInstance().clear();

        //открыли временный файл и записали туда неудалённые значения из базы данных
        GenericFileHandler defragFileHandler = new GenericFileHandler(new RandomAccessFile(tmpFile, "rw"), tmpFile.getName());
        List<DebugRowInfo> debugRowInfos = this.fileHandler.loadAllDataFromFile(this.zclass);
        for (DebugRowInfo dri : debugRowInfos) {
            if (dri.isDeleted() || dri.isTemporary()) {
                continue;

            }
            Object object =  dri.getObject();

            defragFileHandler.add(object);
        }
        //удалили старый файл DB
        boolean wasDeleted = this.fileHandler.deleteFile();
        if (!wasDeleted) {
            tmpFile.delete();
            LOGGER.severe("[" + this.getClass().getName() + "]. " + "Database file cannot be deleted during the defragmentation");
            this.initialise();
            throw new IOException("[" + this.getClass().getName() + "]. " + "DB cannot be deleted. Check the logs.");
        }
        this.fileHandler.close();
        String oldDataBaseName = this.fileHandler.getDBName();
        //копировали на место старого файла новый с неудалённвми данными
        Files.copy(tmpFile.toPath(), FileSystems.getDefault().getPath("", oldDataBaseName),
                StandardCopyOption.REPLACE_EXISTING);
        //закрыли временный файл
        defragFileHandler.close();
        // подсунули текущему filehandlery новый файл
        this.fileHandler = new GenericFileHandler(oldDataBaseName);
        GenericIndex.getInstance().clear();
        //переинициализировали index
        this.initialise();
        LOGGER.info("[" + this.getClass().getName() + "]. " + "Database file has been defragmented");
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

        LOGGER.info("[" + this.getClass().getName() + "]. " + " Rollback DONE. (" + transaction.getUid() + " )");
    }

    @Override
    public long getTotalRecordNumber() {
        return GenericIndex.getInstance().getTotalNumberOfRows();
    }


    private void logInfoObject(final Object object) {
        LOGGER.info("[" + this.getClass().getName() + "]. " + "Read object: " + object);
    }

    private void logInfoListObject(final List<Object> objects) {

        StringBuilder sb = new StringBuilder(300);

        for (Object object : objects) {
            sb.append(object.toString());
            sb.append(System.getProperty("line.separator"));
        }

        LOGGER.info("[" + this.getClass().getName() + "]. " + "Read persons: " + sb);

    }

}
