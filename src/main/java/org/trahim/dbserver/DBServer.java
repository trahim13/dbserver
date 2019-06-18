package org.trahim.dbserver;

import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.FileHandler;
import org.trahim.row.Index;
import org.trahim.row.Person;
import org.trahim.util.DebugRowInfo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

public final class DBServer implements DB {

    private FileHandler fileHandler;

    public DBServer(final String dbFileName) throws IOException {
        this.fileHandler = new FileHandler(dbFileName);
        this.initialise();
    }


    private void initialise() throws IOException {
        this.fileHandler.loadAllDataToIndex();

    }

    @Override
    public void close() throws IOException {
        Index.getInstance().clear();
        this.fileHandler.close();

    }


    @Override
    public void add(Person person) throws IOException, DuplicateNameException {
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

        this.fileHandler.deleteRow(rowNumber);

    }

    @Override
    public Person read(long rowNumber) throws IOException {
        return this.fileHandler.readRow(rowNumber);
    }

    @Override
    public void update(long rowNumber, final Person person) throws IOException, DuplicateNameException {
        this.fileHandler.updateRow(rowNumber,
                person.name, person.age, person.address, person.carPlateNumber, person.description);

    }

    @Override
    public void update(String name, Person person) throws IOException, DuplicateNameException {
        this.fileHandler.update(name,
                person.name, person.age, person.address, person.carPlateNumber, person.description);
    }

    @Override
    public Person search(String name) throws IOException {
        return fileHandler.search(name);
    }

    public List<DebugRowInfo> listAllRowsWithDebug() throws IOException {
        return this.fileHandler.loadAllDataFromFile();
    }

    @Override
    public List<Person> searchWithLeveinshtein(String name, int tolerance) throws IOException {
        return this.fileHandler.searchWithLeveinshtein(name, tolerance);
    }

    @Override
    public List<Person> searchWithRegexp(String regexp) throws IOException {
        return this.fileHandler.searchWithRegex(regexp);
    }

    public void defragmentDatabase() throws IOException, DuplicateNameException {
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
            this.initialise();
            throw new IOException("DB cannot be deleted. Check the logs.");
        }
        this.fileHandler.close();
        String oldDataBaseName = this.fileHandler.getDBName();
        //копировали на место старого файла новый с неудалённвми данными
        Files.copy(tmpFile.toPath(), FileSystems.getDefault().getPath("", oldDataBaseName ),
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