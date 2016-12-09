package ru.zerguy.steamstats;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;

import java.io.IOException;

public class LocationConverter {

    public static final String COUNTRY = "loccountrycode";
    public static final String STATE = "locstatecode";
    public static final String CITY = "loccityid";
    public static final String LOCATION = "location";
    public static final String COORDINATES = "coordinates";

    public final JSONObject id2Country = loadCitiesMap();

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
        // Country
        JSONObject county = (JSONObject) id2Country.get(String.valueOf(user.get(COUNTRY)));
        if (county == null)
            return;

        user.put("country", county.get("name"));


        // State
        JSONObject id2State = (JSONObject) county.get("states");
        if (id2State == null)
            return;

        JSONObject state = (JSONObject) id2State.get(String.valueOf(user.get(STATE)));
        if (state == null) {
            user.put(LOCATION, county.get(COORDINATES));
            return;
        }

        user.put("state", state.get("name"));

        // City
        JSONObject id2City = (JSONObject) state.get("cities");
        if (id2City == null)
            return;

        JSONObject city = (JSONObject) id2City.get(String.valueOf(user.get(CITY)));
        if (city == null) {
            user.put(LOCATION, state.get(COORDINATES));
            return;
        }

        user.put("city", city.get("name"));
        user.put(LOCATION, city.get(COORDINATES));
    }

}
