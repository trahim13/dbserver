package org.trahim.resttestapp;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RESTTestApp {
    public static void main(String[] args) throws IOException {
        RESTTestApp app = new RESTTestApp();

        while (true) {
            app.performTest();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public void performTest() {

        CountDownLatch latch = new CountDownLatch(3);

        ExecutorService executors = Executors.newFixedThreadPool(3);

        Runnable searchThread = () -> {
            while (true) {

                try {
                    performSearchTest();
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        Runnable listAllRecords = () -> {
            while (true) {
                try {
                    listAllRecords();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        Runnable addPerson = () -> {
            while (true) {
                try {
                    addPerson();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        executors.submit(searchThread);
        executors.submit(listAllRecords);
        executors.submit(addPerson);

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void doRequest(final String path, Map<String, String> parameters) throws IOException {
        URL url = null;
        if (parameters != null)
            url = new URL("http://localhost:7001/" + path + "?"
                    + new ParameterBuilder().getStringParameters(parameters));
        else url = new URL("http://localhost:7001/" + path);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(1000);
        connection.setDoOutput(false);


        //read the response
        int responseCode = connection.getResponseCode();
        System.out.println("Response code:" + responseCode);

        if (responseCode == 200) {
            System.out.println(readFromStream(connection.getInputStream()));

        }

        connection.disconnect();

    }

    private void addPerson() throws IOException {
        Map<String, String> parameters = new HashMap<>();
        Random random = new Random();
        int randNumber = random.nextInt(1000000);

        parameters.put("name", "test" + Integer.toString(randNumber));

        parameters.put("age", "32");
        parameters.put("address", "London");
        parameters.put("carplate", "xx-234");
        parameters.put("description", "multitheads desc. TEST.");

        this.doRequest("add", parameters);


    }

    private void listAllRecords() throws IOException {
        this.doRequest("listall", null);


    }

    private void performSearchTest() throws IOException {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("name", "test");

        this.doRequest("searchlevenshtein", parameters);

    }

    private String readFromStream(InputStream inputStream) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line = "";
        StringBuilder sb = new StringBuilder(500);
        sb.append(line);
        while ((line = bufferedReader.readLine()) != null) {
            sb.append(line);
        }

        return sb.toString();
    }

    private class ParameterBuilder {

        public String getStringParameters(Map<String, String> parameters) throws UnsupportedEncodingException {
            StringBuilder sb = new StringBuilder(400);

            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                sb.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                sb.append("=");
                sb.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
                sb.append("&");
            }

            return sb.toString();
        }


    }
}
