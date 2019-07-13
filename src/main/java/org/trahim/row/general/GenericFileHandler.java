package org.trahim.row.general;

import org.trahim.exceptions.DBException;
import org.trahim.exceptions.DuplicateNameException;
import org.trahim.util.Leveinshtein;
import org.trahim.util.OperationUnit;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class GenericFileHandler extends GenericBaseFileHandler {
    public GenericFileHandler(final String dbFileName) throws FileNotFoundException {
        super(dbFileName);
    }

    public GenericFileHandler(final RandomAccessFile randomAccessFile, final String dbFileName) {
        super(randomAccessFile, dbFileName);

    }

    public OperationUnit add(final Object object
    ) throws IOException, DuplicateNameException, DBException {

        writeLock.lock();
        OperationUnit ou = new OperationUnit();

        try {

            String _indexBy = this.schema.indexBy;
            if (_indexBy == null) {
                throw new DBException("indexBy is missing from the schema");
            }

//            String _name = (String) object.getClass().getDeclaredField("_indexBy").get(object);
            //TODO check the DuplicateNameException
//            if (GenericIndex.getInstance().hasInIndex(_name)) {
//                throw new DuplicateNameException(String.format("Name '%s' already exists!", _name));
//            }
            long currentPositionToInsert = this.dbFile.length();
            this.dbFile.seek(currentPositionToInsert);


            int recordLength = 0;
            for (Field field : this.schema.fields) {
                recordLength += getFieldLengthBytype(field, object);
            }

            // isTemporary
            this.dbFile.writeBoolean(true);
            // isDeleted
            this.dbFile.writeBoolean(false);
            //recordLenght
            this.dbFile.writeInt(recordLength);

            //write the record
            for (Field field : this.schema.fields) {
                Object value = object.getClass().getDeclaredField(field.fieldName).get(object);


                if (field.fieldType.equals("String")) {
                    this.dbFile.writeInt(((String) value).length());
                    this.dbFile.write(((String) value).getBytes());
                } else if (field.fieldType.equals("int")) {
                    this.dbFile.writeInt(((Integer) value).intValue());

                }

                //TODO inplements other fields types
            }


            ou.addedRowPosition = currentPositionToInsert;

            return ou;

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IOException("Field related problems during add");
        } finally {


            writeLock.unlock();
        }

    }

    private int getFieldLengthBytype(Field field, Object object) throws NoSuchFieldException, IllegalAccessException {
        int result = 0;

        switch (field.fieldType) {
            case "String":
                Object value = object.getClass().getDeclaredField(field.fieldName).get(object);
                result += ((String) value).length();
                result += 4;
                break;
            case "int":
                result += 4;
                break;
            case "long":
                result += 4;
                break;
            //TODO add more types
        }

        return result;
    }


    public Object readRow(long rowNumber) throws IOException {
        readLock.lock();
        try {
            long bytePosition = GenericIndex.getInstance().getBytePosition(rowNumber);
            if (bytePosition == -1) {
                return null;
            }
            byte[] row = this.readRowRecord(bytePosition);

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(row));

            return readFromByteStream(in, this.zclass);
        } finally {
            readLock.unlock();
        }

    }


    public OperationUnit deleteRow(long rowNumber) throws IOException {
        writeLock.lock();
        try {
            long bytePositionOfRecord = GenericIndex.getInstance().getBytePosition(rowNumber);

            if (rowNumber == -1) {
                throw new IOException("Row does not exists in Index");
            }

            this.dbFile.seek(bytePositionOfRecord);
            // isTemporary
            this.dbFile.writeBoolean(true);

            this.dbFile.seek(bytePositionOfRecord + 1);
            this.dbFile.writeBoolean(true);

            OperationUnit ou = new OperationUnit();
            ou.deletedRowPosition = bytePositionOfRecord;
            return ou;
//            Index.getInstance().remove(rowNumber);
        } finally {
            writeLock.unlock();
        }

    }

    public OperationUnit updateRow(long rowNumber,
                                   final Object object) throws IOException, DuplicateNameException, DBException, NoSuchFieldException, IllegalAccessException {
        writeLock.lock();
        try {
            OperationUnit deletedOperation = this.deleteRow(rowNumber);
            OperationUnit addOperation = this.add(object);

            OperationUnit operation = new OperationUnit();
            operation.deletedRowPosition = deletedOperation.deletedRowPosition;
            operation.addedRowPosition = addOperation.addedRowPosition;
            operation.succesfullOperation = true;
            return operation;
        } finally {
            writeLock.unlock();
        }
    }

    public OperationUnit update(final String indexedFieldName,
                                final Object object) throws IOException, DuplicateNameException, DBException, NoSuchFieldException, IllegalAccessException {
        writeLock.lock();
        try {
            long rowNumber = GenericIndex.getInstance().getRowNumberByIndex(indexedFieldName);
            if (rowNumber == -1) {
                return new OperationUnit();
            }
            return this.updateRow(rowNumber, object);
        } finally {
            writeLock.unlock();
        }

    }

    public Object search(String name) throws IOException {
        long rowNumber = GenericIndex.getInstance().getRowNumberByIndex(name);
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

    public List<Object> searchWithLeveinshtein(String indexedFieldName, int tolerance) throws IOException {
        List<Object> result = new ArrayList<>();
        Set<String> names = GenericIndex.getInstance().getIndexedValues();
        List<String> goodNames = new ArrayList<>();
        for (String storeNames : names) {
            if (Leveinshtein.leveinshteinDistance(storeNames, indexedFieldName) <= tolerance) {
                goodNames.add(storeNames);
            }
        }

        for (String goodName : goodNames) {
            long rowIndex = GenericIndex.getInstance().getRowNumberByIndex(goodName);
            if (rowIndex != -1) {
                Object o = this.readRow(rowIndex);
                result.add(o);
            }
        }
        return result;
    }

    public List<Object> searchWithRegex(String regexp) throws IOException {
        List<Object> result = new ArrayList<>();
        Set<String> names = GenericIndex.getInstance().getIndexedValues();
        List<String> goodNames = new ArrayList<>();
        for (String storeNames : names) {
            if (storeNames.matches(regexp)) {
                goodNames.add(storeNames);
            }
        }

        for (String goodName : goodNames) {
            long rowIndex = GenericIndex.getInstance().getRowNumberByIndex(goodName);
            if (rowIndex != -1) {
                Object p = this.readRow(rowIndex);
                result.add(p);
            }
        }
        return result;
    }

}
