package org.trahim.row;

import java.util.HashMap;

public final class Index {

    private static Index index;
    //row number, byte position
    private HashMap<Long, Long> rowIndex;

    //String name, rowNUmber
    private HashMap<String, Long> nameIndex;

    private  long totalRowNumber = 0;

    private Index() {
        this.rowIndex = new HashMap<>();
        this.nameIndex = new HashMap<>();

    }

    public static Index getInstance() {
        if (index == null) {
            return index = new Index();
        }
        return index;
    }

    public void add(long bytePosition) {
        this.rowIndex.put(totalRowNumber, bytePosition);
        this.totalRowNumber++;
    }

    public long getBytePosition(long rowNumber) {
        return Index.getInstance().rowIndex.getOrDefault(rowNumber, -1L);
    }

    public void remove(long row) {
        this.rowIndex.remove(row);
        this.totalRowNumber--;
    }

    public long getTotalNumberOfRows() {

        return this.totalRowNumber;
    }


    public void addNameToIndex(final String name, long rowIndex) {
        this.nameIndex.put(name, rowIndex);
    }

    public boolean hasNameInIndex(final String name) {
        return this.nameIndex.containsKey(name);
    }

    public long getRowNumberByName(final String name) {
            return this.nameIndex.get(name);
    }

    public void clear() {
        this.totalRowNumber = 0;
        this.rowIndex.clear();
        this.nameIndex.clear();
    }
}
