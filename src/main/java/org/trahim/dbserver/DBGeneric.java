package org.trahim.dbserver;

import org.trahim.exceptions.DBException;
import org.trahim.exceptions.DuplicateNameException;
import org.trahim.transaction.ITransaction;
import org.trahim.util.DebugRowInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface DBGeneric extends Closeable {
    void add(Object object) throws IOException, DuplicateNameException, DBException;

    void delete(long rowNumber) throws IOException;


    void close() throws IOException;

    void update(long rowNumber, final Object object) throws IOException, DuplicateNameException, DBException, NoSuchFieldException, IllegalAccessException;

    void update(String indexedFieldName, final Object object) throws IOException, DuplicateNameException, DBException, NoSuchFieldException, IllegalAccessException;

    Object read(long rowNumber) throws IOException;

    Object search(final String indexedFieldName) throws IOException;

    List<Object> searchWithLeveinshtein(final String indexedFieldName, int tolerance) throws IOException;

    List<Object> searchWithRegexp(final String regexp) throws IOException;

    List<DebugRowInfo> listAllRowsWithDebug() throws IOException;

    ITransaction beginTransaction();

    void commit() throws IOException;

    void rollback() throws IOException;

    long getTotalRecordNumber();

}
