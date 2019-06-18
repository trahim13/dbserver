package org.trahim.dbserver;

import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.Person;
import org.trahim.util.DebugRowInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface DB extends Closeable {
    void add(Person person) throws IOException, DuplicateNameException;

    void delete(long rowNumber) throws IOException;

    Person read(long rowNumber) throws IOException;

    void close() throws IOException;

    void update(long rowNumber, final Person person) throws IOException, DuplicateNameException;
    void update(String name, final Person person) throws IOException, DuplicateNameException;

    Person search(final String name) throws IOException;

    List<Person> searchWithLeveinshtein(final String name, int tolerance) throws IOException;

    List<Person> searchWithRegexp(final String regexp) throws IOException;

    List<DebugRowInfo> listAllRowsWithDebug() throws IOException;


}
