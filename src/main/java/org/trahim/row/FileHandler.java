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

        this.dbFile.seek(this.dbFile.length());

        int length =
                4 + name.length() +
                        4 +
                        4 + address.length() +
                        4 + carPlateNumber.length() +
                        4 + description.length();


        this.dbFile.writeBoolean(false);
        this.dbFile.write(length);


        this.dbFile.writeInt(name.length());
        this.dbFile.write(name.getBytes());

        this.dbFile.write(age);

        this.dbFile.writeInt(address.length());
        this.dbFile.write(address.getBytes());

        this.dbFile.writeInt(carPlateNumber.length());
        this.dbFile.write(carPlateNumber.getBytes());

        this.dbFile.writeInt(description.length());
        this.dbFile.write(description.getBytes());

        return true;

    }


    public Person readRow(int rowNumber) throws IOException {
        byte [] row = this.readRowRecord(rowNumber);
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

    public byte[] readRowRecord(int rowNumber) throws IOException {
        this.dbFile.seek(0);

        if (this.dbFile.readBoolean()) {
            return new byte[0];
        }

        this.dbFile.seek(rowNumber + 1);
        int recordLength = this.dbFile.readInt();
        this.dbFile.seek(rowNumber + 5);
        byte[] data = new byte[recordLength];
        this.dbFile.read(data);

        return data;
    }
}
