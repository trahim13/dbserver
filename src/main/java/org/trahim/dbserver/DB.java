package org.trahim.dbserver;

import org.trahim.exceptions.DuplicateNameException;
import org.trahim.row.Person;

import java.io.IOException;

public interface DB {
    void add(Person person) throws IOException, DuplicateNameException;

    void delete(long rowNumber) throws IOException;

    Person read(long rowNumber) throws IOException;

    void close() throws IOException;

    void update(long rowNumber, final Person person) throws IOException, DuplicateNameException;
    void update(String name, final Person person) throws IOException, DuplicateNameException;


}
