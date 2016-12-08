package ru.zerguy.steamstats;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.SearchHit;
import org.json.simple.JSONObject;

import java.io.IOException;

public class LocationConverterApp {

    public static final String COUNTRY = "loccountrycode";
    public static final String STATE = "locstatecode";
    public static final String CITY = "loccityid";
    public static final String LOCATION = "location";
    public static final String COORDINATES = "coordinates";

    public final JSONObject id2City = loadCitiesMap();

    private final TransportClient client = ElasticSearchConnectionFactory.createConnection();
    private final String CITIES_MAP_FILE = "steam_countries.json";

    private JSONObject loadCitiesMap() {
        String result = "";

        ClassLoader classLoader = getClass().getClassLoader();
        try {
            result = IOUtils.toString(classLoader.getResourceAsStream(CITIES_MAP_FILE));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return App.parseString(result);
    }

    private void start() {
        SearchResponse response = client.prepareSearch(App.INDEX).setTypes(App.TYPE).setFrom(27516).setSize(5).get();

        for (SearchHit hit : response.getHits()) {
            String id = hit.id();
            System.out.println(id);
            GetResponse getResponse = client.prepareGet(App.INDEX, App.TYPE, id).get();
            JSONObject user = new JSONObject(getResponse.getSource());
            user.remove("stats");

            JSONObject county = (JSONObject) id2City.get(user.get(COUNTRY));
            if (user.containsKey(STATE)) {
                user.put(LOCATION, county.get(COORDINATES));
                return;
            }

            JSONObject state = (JSONObject) id2City.get(user.get(STATE));
            if (user.containsKey(CITY)) {
                user.put(LOCATION, state.get(COORDINATES));
                return;
            }

            JSONObject city = (JSONObject) id2City.get(user.get(CITY));
            user.put(LOCATION, city.get(COORDINATES));

            client.prepareIndex(App.INDEX, App.TYPE, id).setSource(user.toString()).get();
        }


    }

    public static void main(String[] args) {
        new LocationConverterApp().start();
    }

}
