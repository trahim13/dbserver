package org.trahim.row;

import org.trahim.exceptions.DuplicateNameException;
import org.trahim.util.Leveinshtein;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FileHandler extends BaseFileHandler {


    public FileHandler(final String dbFileName) throws FileNotFoundException {
        super(dbFileName);
    }

    public FileHandler(final RandomAccessFile randomAccessFile, final String dbFileName) {
        super(randomAccessFile, dbFileName);

    }

    public void add(String name,
                    int age,
                    String address,
                    String carPlateNumber,
                    String description
    ) throws IOException, DuplicateNameException {

        writeLock.lock();
        try {
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
            Index.getInstance().addNameToIndex(name, Index.getInstance().getTotalNumberOfRows() - 1);
        } finally {
            writeLock.unlock();
        }

    }


    public Person readRow(long rowNumber) throws IOException {
        readLock.lock();
        try {
            long bytePosition = Index.getInstance().getBytePosition(rowNumber);
            if (bytePosition == -1) {
                return null;
            }
            byte[] row = this.readRowRecord(bytePosition);

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(row));

            return readFromByteStream(in);
        } finally {
            readLock.unlock();
        }

    }


    public void deleteRow(long rowNumber) throws IOException {
        writeLock.lock();
        try {
            long bytePositionOfRecord = Index.getInstance().getBytePosition(rowNumber);

            if (rowNumber == -1) {
                throw new IOException("Row does not exists in Index");
            }

            this.dbFile.seek(bytePositionOfRecord);
            this.dbFile.writeBoolean(true);

            Index.getInstance().remove(rowNumber);
        } finally {
            writeLock.unlock();
        }

    }

    public void updateRow(long rowNumber,
                          String name,
                          int age,
                          String address,
                          String carPlateNumber,
                          String description) throws IOException, DuplicateNameException {
        writeLock.lock();
        try {
            this.deleteRow(rowNumber);
            this.add(name, age, address, carPlateNumber, description);
        } finally {
            writeLock.unlock();
        }
    }

    public void update(final String nameToModify,
                       String name,
                       int age,
                       String address,
                       String carPlateNumber,
                       String description) throws IOException, DuplicateNameException {
        writeLock.lock();
        try {
            long rowNumber = Index.getInstance().getRowNumberByName(nameToModify);
            this.updateRow(rowNumber, name, age, address, carPlateNumber, description);
        } finally {
            writeLock.unlock();
        }

    }

    public Person search(String name) throws IOException {
        long rowNumber = Index.getInstance().getRowNumberByName(name);
        if (rowNumber == -1) {
            return null;
        }

        return this.readRow(rowNumber);
//        LongStream.range(0, Index.getInstance().getTotalNumberOfRows())
//                .forEach(i->{
//                    Person p = this.readRow(i);
//                    if (p.name.equals(name)) {
//                        result.add(p);
//                    }
//                });

    }

    public List<Person> searchWithLeveinshtein(String name, int tolerance) throws IOException {
        List<Person> result = new ArrayList<>();
        Set<String> names = Index.getInstance().getNames();
        List<String> goodNames = new ArrayList<>();
        for (String storeNames : names) {
            if (Leveinshtein.leveinshteinDistance(storeNames, name) <= tolerance) {
                goodNames.add(storeNames);
            }
        }

        for (String goodName : goodNames) {
            long rowIndex = Index.getInstance().getRowNumberByName(goodName);
            if (rowIndex != -1) {
                Person p = this.readRow(rowIndex);
                result.add(p);
            }
        }
        return result;
    }

    public List<Person> searchWithRegex(String regexp) throws IOException {
        List<Person> result = new ArrayList<>();
        Set<String> names = Index.getInstance().getNames();
        List<String> goodNames = new ArrayList<>();
        for (String storeNames : names) {
            if (storeNames.matches(regexp)) {
                goodNames.add(storeNames);
            }
        }

        for (String goodName : goodNames) {
            long rowIndex = Index.getInstance().getRowNumberByName(goodName);
            if (rowIndex != -1) {
                Person p = this.readRow(rowIndex);
                result.add(p);
            }
        }
        return result;
    }
}
