package org.trahim.server;

import io.javalin.http.Handler;
import org.trahim.dbserver.DB;
import org.trahim.dbserver.DBServer;
import org.trahim.row.Person;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.LongStream;

public final class DBController {

    private static DB DATABASE;

    static {
        try {
            DATABASE = new DBServer("test.db");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Handler fetchAllRecords = ctx->{
        long totalRecordNumber = DATABASE.getTotalRecordNumber();
        List<String> result = new ArrayList<>();

        LongStream.range(0, totalRecordNumber)
                .forEach(i->{
                    try {
                        Person p = DATABASE.read(i);
                        result.add(p.toJSON());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

        ctx.json(result);
    };

    public static Handler addPerson= ctx-> {

        DATABASE.beginTransaction();

        long totalRecordNumber = DATABASE.getTotalRecordNumber();
        String name = ctx.queryParam("name");
        int age = Integer.parseInt(ctx.queryParam("age"));
        String address = ctx.queryParam("address");
        String carplate = ctx.queryParam("carplate");
        String description = ctx.queryParam("description");


        if (name == null || address == null || carplate == null || description == null) {
            ctx.json("{\"Error\": \"Parameter is missing}\"");
            return;
        }

        Person p = new Person(name, age, address, carplate, description);
        DATABASE.add(p);
        DATABASE.commit();

        ctx.json(true);
    };


    public static Handler searchLevenshtein= ctx-> {
        if (ctx.queryParam("name") == null) {
            ctx.json("{\"Error\": \"Parameter is missing (name)\"}");
            return;
        }
        String name = ctx.queryParam("name");

        LinkedList<String> result = new LinkedList<>();

        List<Person> persons = DATABASE.searchWithLeveinshtein(name, 1);
        persons.forEach(i->{
            result.add(i.toJSON());
        });

        ctx.json(result);
    };

}
