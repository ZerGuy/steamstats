package ru.zerguy.steamstats;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.SearchHit;

public class LocationConverterApp {

    private final TransportClient client = ElasticSearchConnectionFactory.createConnection();

    private void start() {
        SearchResponse response = client.prepareSearch("test").setTypes("player").get();

        for (SearchHit hit : response.getHits()) {
            String id = (String) hit.getSource().get("_id");
            System.out.println(id);
        }

    }

    public static void main(String[] args) {
        new LocationConverterApp().start();
    }

}
