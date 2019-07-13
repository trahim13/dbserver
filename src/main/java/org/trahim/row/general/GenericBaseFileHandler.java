package org.trahim.row.general;

import org.trahim.row.specific.Index;
import org.trahim.util.DebugRowInfo;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GenericBaseFileHandler {
    RandomAccessFile dbFile;
    private final String dbFileName;
    protected Schema schema;
    protected Class zclass;


    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    final Lock readLock = readWriteLock.readLock();
    final Lock writeLock = readWriteLock.writeLock();

    private final static int HEADER_INFO_SPACE = 100;

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    public void setZClass(final Class zclass) {
        this.zclass = zclass;
    }


    GenericBaseFileHandler(String dbFileName) throws FileNotFoundException {
        this.dbFileName = dbFileName;
        this.dbFile = new RandomAccessFile(dbFileName, "rw");
    }

    GenericBaseFileHandler(final RandomAccessFile randomAccessFile, final String dbFileName) {
        this.dbFileName = dbFileName;
        this.dbFile = randomAccessFile;
    }


    public void initialise() throws IOException {
        if (this.dbFile.length() == 0) {
            this.setDBVersion();

        } else {
            String dbVersion = this.getDBVersion();
            System.out.println("DB version: " + dbVersion);
        }
    }

    public void loadAllDataToIndex(final Class zclass) throws IOException {

        readLock.lock();
        try {
            if (this.dbFile.length() == 0) {
                return;
            }

            long currentPos = HEADER_INFO_SPACE;
            long rowNumber = 0;
            long deletedRows = 0;
            long temporaryRows = 0;

            synchronized (this) {
                while (currentPos < this.dbFile.length()) {

                    this.dbFile.seek(currentPos);

                    boolean isTemporary = this.dbFile.readBoolean();
                    if (isTemporary) {
                        temporaryRows += 1;
                    }

                    currentPos += 1;
                    this.dbFile.seek(currentPos);
                    boolean isDeleted = this.dbFile.readBoolean();

                    if (!isDeleted) {
                        GenericIndex.getInstance().add(currentPos);
                    } else {
                        deletedRows++;
                    }

                    currentPos += 1;
                    this.dbFile.seek(currentPos);
                    int recordLength = this.dbFile.readInt();
                    currentPos += 4;

                    this.dbFile.seek(currentPos);
                    if (!isDeleted && !isTemporary) {
                        byte[] b = new byte[recordLength];
                        this.dbFile.read(b);

                        Object object = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)), zclass);
                        String _name = (String) object.getClass().getDeclaredField("pname").get(object);

                        GenericIndex.getInstance().addNameToIndex(_name, rowNumber);
                        rowNumber++;
                    }

                    currentPos += recordLength;
                }
            }

            System.out.println("After startup: loadAllDataToIndex() -> Total row number in Database: " + rowNumber);
            System.out.println("After startup: loadAllDataToIndex() -> Total deleted row number in DataBase: " + deletedRows);
            System.out.println("After startup: loadAllDataToIndex() -> Total temporary row number in DataBase: " + temporaryRows);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }  finally {
            readLock.unlock();
        }

    }


    Object readFromByteStream(final DataInputStream in, final Class zclass) throws IOException {

        Object result = null;

        try {
            result = Class.forName(zclass.getCanonicalName())
                    .newInstance();

            for (Field field : this.schema.fields) {
                if (field.fieldType.equalsIgnoreCase("String")) {
                    int fieldLength = in.readInt();
                    byte[] b = new byte[fieldLength];
                    in.read(b);
                    String value = new String(b);
                    //set the value into result object
                    result.getClass().getDeclaredField(field.fieldName).set(result, value);

                } else if (field.fieldType.equalsIgnoreCase("int")) {
                    int value = in.readInt();
                    //set the value into result object
                    result.getClass().getDeclaredField(field.fieldName).set(result, value);

                }

                //TODO implements other fields type
            }


        } catch (InstantiationException | ClassNotFoundException | IllegalAccessException |NoSuchFieldException e) {
            throw new IOException(e.getMessage());
        }


        return result;
    }


    byte[] readRowRecord(long bytePositionOfRow) throws IOException {
        readLock.lock();
        try {
            synchronized (this) {
                this.dbFile.seek(bytePositionOfRow);
                this.dbFile.readBoolean(); // temporary

                this.dbFile.seek(bytePositionOfRow + 1);
                if (this.dbFile.readBoolean()) {
                    return new byte[0];
                }

                this.dbFile.seek(bytePositionOfRow + 2);
                int recordLength = this.dbFile.readInt();

                this.dbFile.seek(bytePositionOfRow + 6);
                byte[] data = new byte[recordLength];
                this.dbFile.read(data);

                return data;
            }
        } finally {
            readLock.unlock();
        }
    }


    public void close() throws IOException {
        this.dbFile.close();
    }

    public List<DebugRowInfo> loadAllDataFromFile(final Class zclass) throws IOException {
        writeLock.lock();
        try {
            if (this.dbFile.length() == 0) {
                return new ArrayList<>();
            }

            ArrayList<DebugRowInfo> result;
            synchronized (this) {
                result = new ArrayList<>();
                long currentPosition = HEADER_INFO_SPACE;

                while (currentPosition < this.dbFile.length()) {
                    this.dbFile.seek(currentPosition);
                    boolean isTemporary = this.dbFile.readBoolean();

                    currentPosition += 1;
                    this.dbFile.seek(currentPosition);
                    boolean isDeleted = this.dbFile.readBoolean();

                    currentPosition += 1;
                    this.dbFile.seek(currentPosition);
                    int recordLength = this.dbFile.readInt();

                    currentPosition += 4;
                    byte[] b = new byte[recordLength];
                    this.dbFile.read(b);

                    Object object = readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)), zclass);
                    result.add(new DebugRowInfo(object, isDeleted, isTemporary));
                    currentPosition += recordLength;
                }
            }

            return result;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean deleteFile() throws IOException {
        writeLock.lock();
        try {
            this.dbFile.close();
            if (new File(this.dbFileName).delete()) {
                System.out.println("The file has deleted");
                return true;
            } else {
                System.out.println("The file NOT deleted");
                return false;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public String getDBName() {

        return this.dbFileName;
    }


    public void commit(List<Long> newRows, List<Long> deletedRows) throws IOException {
        writeLock.lock();
        try {
            for (Long position : newRows) {
                dbFile.seek(position);
                this.dbFile.writeBoolean(false);

                // re-read the record
                byte[] b = this.readRowRecord(position);
                Object object = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)), this.zclass);
                //add it to the index
                String _name = (String) object.getClass().getDeclaredField(this.schema.indexBy).get(object);

                GenericIndex.getInstance().addNameToIndex(_name, Index.getInstance().getTotalNumberOfRows());
                GenericIndex.getInstance().add(position);


            }
            for (Long position : deletedRows) {
                dbFile.seek(position);
                this.dbFile.writeBoolean(false);
                GenericIndex.getInstance().removeByFilePosition(position);
            }
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IOException(e.getMessage());
        } finally {
            writeLock.unlock();
        }

    }

    public void rollback(List<Long> newRows, List<Long> deletedRows) throws IOException {
        writeLock.lock();

        try {
            for (Long position : newRows) {
                this.dbFile.seek(position);
                this.dbFile.writeBoolean(false);

                this.dbFile.seek(position + 1);
                this.dbFile.writeBoolean(true);

                GenericIndex.getInstance().removeByFilePosition(position);

            }

            for (Long position : deletedRows) {
                this.dbFile.seek(position);
                this.dbFile.writeBoolean(false);
                this.dbFile.seek(position + 1);
                this.dbFile.writeBoolean(false);

                byte[] b = this.readRowRecord(position);
                Object object = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)), this.zclass);
                //add it to the index
                String _name = (String) object.getClass().getDeclaredField(this.schema.indexBy).get(object);
                GenericIndex.getInstance().addNameToIndex(_name, Index.getInstance().getTotalNumberOfRows());
                GenericIndex.getInstance().add(position);

            }
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IOException(e.getMessage());
        } finally {
            writeLock.unlock();

        }
    }

    private void setDBVersion() throws IOException {

        this.dbFile.seek(0);
        String VERSION = "0.1";
        this.dbFile.write(VERSION.getBytes());
        char[] chars = new char[HEADER_INFO_SPACE - VERSION.length()];
        Arrays.fill(chars, ' ');
        this.dbFile.write(new String(chars).getBytes());

    }


    private String getDBVersion() throws IOException {
        readLock.lock();
        try {
            this.dbFile.seek(0);
            byte[] b = new byte[HEADER_INFO_SPACE];
            this.dbFile.read(b);
            return new String(b).trim();
        } finally {
            readLock.unlock();
        }
    }

}
