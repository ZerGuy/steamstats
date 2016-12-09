package ru.zerguy.steamstats;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.json.simple.JSONObject;

import java.io.IOException;

public class LocationConverter {

    public static final String COUNTRY = "loccountrycode";
    public static final String STATE = "locstatecode";
    public static final String CITY = "loccityid";
    public static final String LOCATION = "location";
    public static final String COORDINATES = "coordinates";

    public final JSONObject id2City = loadCitiesMap();

    private final TransportClient client = ElasticSearchConnectionFactory.createConnection();
    private final String CITIES_MAP_FILE = "steam_countries.json";

    public JSONObject loadCitiesMap() {
        String result = "";

        ClassLoader classLoader = getClass().getClassLoader();
        try {
            result = IOUtils.toString(classLoader.getResourceAsStream(CITIES_MAP_FILE));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return App.parseString(result);
    }

    public void addLocation(JSONObject user) {
        JSONObject county = (JSONObject) id2City.get(user.get(COUNTRY));
        if (county == null)
            return;

        JSONObject state = (JSONObject) id2City.get(user.get(STATE));
        if (state == null) {
            user.put(LOCATION, county.get(COORDINATES));
            return;
        }

        JSONObject city = (JSONObject) id2City.get(user.get(CITY));
        if (city == null) {
            user.put(LOCATION, state.get(COORDINATES));
            return;
        }

        user.put(LOCATION, city.get(COORDINATES));
    }

}
