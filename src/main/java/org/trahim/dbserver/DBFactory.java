package org.trahim.dbserver;

import org.trahim.exceptions.DBException;

import java.io.IOException;

public final class DBFactory {
    public static DB getSpecificDB(final String dbFileName) throws IOException {
        return new DBServer(dbFileName);
    }

    public static DBGeneric getGenericDB(final String dbFileName,
                                  final String schema,
                                  final Class zclass) throws IOException, DBException {
        return new DBGenericServer(dbFileName,  schema, zclass);
    }


}
