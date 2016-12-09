package ru.zerguy.steamstats;

import org.elasticsearch.client.transport.TransportClient;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TestApp {

    public static final String INDEX = "test";
    public static final String TYPE = "player";

    private final TransportClient client = ElasticSearchConnectionFactory.createConnection();


    private void start() throws IOException {
        client.prepareIndex(INDEX, TYPE, "1").setSource(jsonBuilder()
                .startObject()
                .field("user", "User")
                .field("location", "50.910111,4.510488")
                .endObject()
        ).get();
    }

    public static void main(String[] args) throws IOException {
        TestApp app = new TestApp();
        app.start();
    }
}
