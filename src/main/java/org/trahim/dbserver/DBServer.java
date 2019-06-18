package org.trahim.dbserver;

import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.FileHandler;
import org.trahim.row.Index;
import org.trahim.row.Person;
import org.trahim.util.DebugRowInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DBServer implements DB {

    private FileHandler fileHandler;

    private final Logger LOGGER = Logger.getLogger("DBServer");
    private final static String LOG_FILE_NAME = "config.properties";
    private final static String LOG_LEVEL = "LOG_LEVEL";

    public DBServer(final String dbFileName) throws IOException {
        this.fileHandler = new FileHandler(dbFileName);
        this.initialise();
    }


    private void initialise() throws IOException {

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
        Index.getInstance().clear();
        this.fileHandler.close();

    }


    @Override
    public void add(Person person) throws IOException, DuplicateNameException {
        LOGGER.info("Adding person: " + person);

        this.fileHandler.add(
                person.name,
                person.age,
                person.address,
                person.carPlateNumber,
                person.description);

    }

    @Override
    public void delete(long rowNumber) throws IOException {
        if (rowNumber < 0) {
            throw new IOException("Row number is less then 0");
        }
        LOGGER.info("Deleting person. Row number: " + rowNumber);

        this.fileHandler.deleteRow(rowNumber);

    }

    @Override
    public Person read(long rowNumber) throws IOException {
        LOGGER.info("Reading row. Row number: " + rowNumber);
        return this.fileHandler.readRow(rowNumber);
    }

    @Override
    public void update(long rowNumber, final Person person) throws IOException, DuplicateNameException {
        LOGGER.info("Updating person. Row number: " + rowNumber + ". Person " + person);
        this.fileHandler.updateRow(rowNumber,
                person.name, person.age, person.address, person.carPlateNumber, person.description);

    }

    @Override
    public void update(String name, Person person) throws IOException, DuplicateNameException {
        LOGGER.info("Updating person. Name: " + name + ". Person " + person);
        this.fileHandler.update(name,
                person.name, person.age, person.address, person.carPlateNumber, person.description);
    }

    @Override
    public Person search(String name) throws IOException {
        LOGGER.info("Searching person: " + name);
        return fileHandler.search(name);
    }

    @Override
    public List<DebugRowInfo> listAllRowsWithDebug() throws IOException {
        return this.fileHandler.loadAllDataFromFile();
    }

    @Override
    public List<Person> searchWithLeveinshtein(String name, int tolerance) throws IOException {
        LOGGER.info("Searching with Levenshtein. Name: " + name + ". tolerance: " + tolerance);
        return this.fileHandler.searchWithLeveinshtein(name, tolerance);
    }

    @Override
    public List<Person> searchWithRegexp(String regexp) throws IOException {
        LOGGER.info("Searching with Regex. Regex: " + regexp);
        return this.fileHandler.searchWithRegex(regexp);
    }

    public void defragmentDatabase() throws IOException, DuplicateNameException {
        LOGGER.info("Defragmenting Database");

        //создали временный файл
        File tmpFile = File.createTempFile("defrag", "dat");
        Index.getInstance().clear();

        //открыли временный файл и записали туда неудалённые значения из базы данных
        FileHandler defragFileHandler = new FileHandler(new RandomAccessFile(tmpFile, "rw"), tmpFile.getName());
        List<DebugRowInfo> debugRowInfos = this.fileHandler.loadAllDataFromFile();
        for (DebugRowInfo dri : debugRowInfos) {
            if (!dri.isDeleted()) {
                continue;

            }
            Person p = dri.getPerson();

            defragFileHandler.add(p.name, p.age, p.address, p.carPlateNumber, p.description);
        }
        //удалили старый файл DB
        boolean wasDeleted = this.fileHandler.deleteFile();
        if (!wasDeleted) {
            tmpFile.delete();
            LOGGER.severe("Database file cannot be deleted during the defragmentation");
            this.initialise();
            throw new IOException("DB cannot be deleted. Check the logs.");
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
    }
}