package org.trahim.row;

import java.io.*;

public class FileHandler {

    private RandomAccessFile dbFile;

    public FileHandler(String dbFileName) throws FileNotFoundException {
        this.dbFile = new RandomAccessFile(dbFileName, "rw");
    }

    public void close() throws IOException {
        this.dbFile.close();
    }

    public boolean add(String name,
                       int age,
                       String address,
                       String carPlateNumber,
                       String description
    ) throws IOException {

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

        return true;

    }


    public Person readRow(int rowNumber) throws IOException {

        long bytePosition= Index.getInstance().getBytePosition(rowNumber);
        if (bytePosition == -1) {
            return null;
        }
        byte [] row = this.readRowRecord(bytePosition);
        Person person = new Person();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(row));

        int nameLength = in.readInt();
        byte[] b = new byte[nameLength];
        in.read(b);
        person.name = new String(b);



        person.age = in.readInt();

        b = new byte[in.readInt()];
        in.read(b);
        person.address = new String(b);


        b = new byte[in.readInt()];
        in.read(b);
        person.carPlateNumber = new String(b);


        b = new byte[in.readInt()];
        in.read(b);
        person.description = new String(b);

        return person;

    }

    public byte[] readRowRecord(long bytePositionOfRow) throws IOException {
        this.dbFile.seek(bytePositionOfRow);

        if (this.dbFile.readBoolean()) {
            return new byte[0];
        }

        this.dbFile.seek(bytePositionOfRow + 1);
        int recordLength = this.dbFile.readInt();

        this.dbFile.seek(bytePositionOfRow + 5);
        byte[] data = new byte[recordLength];
        this.dbFile.read(data);

        return data;
    }

    public void loadAllDataToIndex() throws IOException {
        if (this.dbFile.length() == 0) {
            return;
        }

        long currentPos = 0;
        long rowNumber = 0;
        long deletedRows = 0;

        while (currentPos < this.dbFile.length()) {
            this.dbFile.seek(currentPos);
            boolean isDeleted = this.dbFile.readBoolean();

            if (!isDeleted) {
                Index.getInstance().add(currentPos);
                rowNumber++;
            } else {
                deletedRows++;
            }

            currentPos += 1;
            this.dbFile.seek(currentPos);
            int recordLength = this.dbFile.readInt();
            currentPos += 4;
            currentPos += recordLength;
        }

        System.out.println("After startup: loadAllDataToIndex() -> Total row number in Database: " + rowNumber);
        System.out.println("After startup: loadAllDataToIndex() -> Total deleted row number in DataBase: " + deletedRows);

    }

    public void deleteRow(int rowNumber) throws IOException {
        long bytePositionOfRecord = Index.getInstance().getBytePosition(rowNumber);

        if (rowNumber == -1) {
            throw new IOException("Row does not exists in Index");
        }

        this.dbFile.seek(bytePositionOfRecord);
        this.dbFile.writeBoolean(true);

        Index.getInstance().remove(rowNumber);

    }
}
