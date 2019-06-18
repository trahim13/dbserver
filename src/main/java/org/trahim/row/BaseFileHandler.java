package org.trahim.row;


import org.trahim.util.DebugRowInfo;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BaseFileHandler {

    RandomAccessFile dbFile;
    String dbFileName = "";

    BaseFileHandler(String dbFileName) throws FileNotFoundException {
        this.dbFileName = dbFileName;
        this.dbFile = new RandomAccessFile(dbFileName, "rw");
    }

    public BaseFileHandler(final RandomAccessFile randomAccessFile, final String dbFileName) {
        this.dbFileName = dbFileName;
        this.dbFile = randomAccessFile;
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
            } else {
                deletedRows++;
            }

            currentPos += 1;
            this.dbFile.seek(currentPos);
            int recordLength = this.dbFile.readInt();
            currentPos += 4;

            this.dbFile.seek(currentPos);
            if (!isDeleted) {
                byte[] b = new byte[recordLength];
                this.dbFile.read(b);
                Person p = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)));
                Index.getInstance().addNameToIndex(p.name, rowNumber);
                rowNumber++;
            }

            currentPos += recordLength;
        }

        System.out.println("After startup: loadAllDataToIndex() -> Total row number in Database: " + rowNumber);
        System.out.println("After startup: loadAllDataToIndex() -> Total deleted row number in DataBase: " + deletedRows);

    }


    Person readFromByteStream(final DataInputStream in) throws IOException {
        Person person = new Person();

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


    byte[] readRowRecord(long bytePositionOfRow) throws IOException {
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


    public void close() throws IOException {
        this.dbFile.close();
    }

    public List<DebugRowInfo> loadAllDataFromFile() throws IOException {
        if (this.dbFile.length() == 0) {
            return new ArrayList<>();
        }

        ArrayList<DebugRowInfo> result = new ArrayList<>();
        long currentPosition = 0;

        while (currentPosition < this.dbFile.length()) {
            this.dbFile.seek(currentPosition);

            boolean isDeleted = this.dbFile.readBoolean();
            currentPosition += 1;
            this.dbFile.seek(currentPosition);
            int recordLength = this.dbFile.readInt();
            currentPosition += 4;
            byte[] b = new byte[recordLength];
            this.dbFile.read(b);

            Person person = readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)));
            result.add(new DebugRowInfo(person, isDeleted));
            currentPosition += recordLength;
        }

        return result;
    }

    public boolean deleteFile() throws IOException {
        this.dbFile.close();
        if (new File(this.dbFileName).delete()) {
            System.out.println("The file has deleted");
            return true;
        } else {
            System.out.println("The file NOT deleted");
            return false;

        }
    }

   public String getDBName() {

        return this.dbFileName;
    }

}
