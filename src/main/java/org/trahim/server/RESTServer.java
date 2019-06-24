package org.trahim.server;

import io.javalin.Javalin;

public final class RESTServer {

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7001);
        app.get("/listall", DBController.fetchAllRecords);
        app.get("/add", DBController.addPerson);
        app.get("/searchlevenshtein", DBController.searchLevenshtein);
    }
}
