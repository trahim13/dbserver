package org.trahim.dbserver;

import org.trahim.row.FileHandler;
import org.trahim.row.Index;
import org.trahim.row.Person;

import java.io.IOException;

public final class DBServer implements DB {

    private FileHandler fileHandler;

    public DBServer(final String dbFileName) throws IOException {
        this.fileHandler = new FileHandler(dbFileName);
        this.fileHandler.loadAllDataToIndex();
    }

    @Override
    public void close() throws IOException {
        Index.getInstance().clear();
        this.fileHandler.close();

    }

    @Override
    public void add(String name,
                    int age,
                    String address,
                    String carPlateNumber,
                    String description) throws IOException {
        this.fileHandler.add(name, age, address, carPlateNumber, description);

    }

    @Override
    public void delete(int rowNumber) throws IOException {

    }

    @Override
    public Person read(int rowNumber) throws IOException {
        return this.fileHandler.readRow(rowNumber);
    }

}
