package org.trahim.row;

import org.trahim.exceptions.DuplicateNameException;

import java.io.*;

public class FileHandler extends BaseFileHandler {



    public FileHandler(String dbFileName) throws FileNotFoundException {
        super(dbFileName);
    }



    public boolean add(String name,
                       int age,
                       String address,
                       String carPlateNumber,
                       String description
    ) throws IOException, DuplicateNameException {

        if (Index.getInstance().hasNameInIndex(name)) {
            throw new DuplicateNameException(String.format("Name '%s' already exists!", name));
        }
        long currentPositionToInsert = this.dbFile.length();
        this.dbFile.seek(currentPositionToInsert);

        int length =
                        4 + name.length() +
                        4 +
                        4 + address.length() +
                        4 + carPlateNumber.length() +
                        4 + description.length();


        this.dbFile.writeBoolean(false);
        this.dbFile.writeInt(length);


        this.dbFile.writeInt(name.length());
        this.dbFile.write(name.getBytes());

        this.dbFile.writeInt(age);

        this.dbFile.writeInt(address.length());
        this.dbFile.write(address.getBytes());

        this.dbFile.writeInt(carPlateNumber.length());
        this.dbFile.write(carPlateNumber.getBytes());

        this.dbFile.writeInt(description.length());
        this.dbFile.write(description.getBytes());

        Index.getInstance().add(currentPositionToInsert);
        Index.getInstance().addNameToIndex(name, Index.getInstance().getTotalNumberOfRows()-1);

        return true;

    }


    public Person readRow(long rowNumber) throws IOException {

        long bytePosition= Index.getInstance().getBytePosition(rowNumber);
        if (bytePosition == -1) {
            return null;
        }
        byte [] row = this.readRowRecord(bytePosition);

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(row));

        return readFromByteStream(in);

    }


    public void deleteRow(long rowNumber) throws IOException {
        long bytePositionOfRecord = Index.getInstance().getBytePosition(rowNumber);

        if (rowNumber == -1) {
            throw new IOException("Row does not exists in Index");
        }

        this.dbFile.seek(bytePositionOfRecord);
        this.dbFile.writeBoolean(true);

        Index.getInstance().remove(rowNumber);

    }

    public void updateRow(long rowNumber,
                          String name,
                          int age,
                          String address,
                          String carPlateNumber,
                          String description) throws IOException, DuplicateNameException {
        this.deleteRow(rowNumber);
        this.add(name, age, address, carPlateNumber, description);
    }

    public void update(final String nameToModify,
                       String name,
                       int age,
                       String address,
                       String carPlateNumber,
                       String description) throws IOException, DuplicateNameException {
        long rowNumber = Index.getInstance().getRowNumberByName(nameToModify);
       this.updateRow(rowNumber, name, age, address, carPlateNumber, description);

    }
}
