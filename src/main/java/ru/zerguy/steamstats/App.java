package ru.zerguy.steamstats;

import org.elasticsearch.client.transport.TransportClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.*;

public class App {

    public static final String INDEX = "steam";
    public static final String TYPE = "player";

    public static final Long START_STEAM_ID = 76561198023043869L; //ZerGuy

    public static final JSONParser jsonParser = new JSONParser();

    public static final int BATCH_MAX_SIZE = 100;

    private final Queue<Long> idsToProceed = new LinkedList<>();
    private final Set<Long> proceededIds = new HashSet<>();

    private final TransportClient client = ElasticSearchConnectionFactory.createConnection();
    private final LocationConverter locationConverter = new LocationConverter();

    private void start() {
        if (client == null)
            return;

        System.out.println("Connection created");

        createIndex();

        idsToProceed.add(START_STEAM_ID);

        while (!idsToProceed.isEmpty()) {
            loadMoreUsers();
        }

        System.out.println("Closing connection");
        client.close();
    }

    private void loadMoreUsers() {
        List<Long> batch = generateBatch();
        Map<Long, JSONObject> id2JSON = loadUsersBatch(batch);

        for (Long userId : batch) {
            proceedUser(userId, id2JSON);
        }
    }

    private void proceedUser(Long userId, Map<Long, JSONObject> id2JSON) {
        if (isInIndex(userId)) {
            loadUserFriends(userId, new JSONObject());
            return;
        }

        proceededIds.add(userId);
        System.out.println("Proceeding       " + userId);

        JSONObject userJson = id2JSON.get(userId);
        loadUserFriends(userId, userJson);
        loadUserGameStats(userId, userJson);
        locationConverter.addLocation(userJson);

        client.prepareIndex(INDEX, TYPE, userId.toString()).setSource(userJson.toString()).get();
        System.out.println("Added            " + userId);
        System.out.println("Users proceeded: " + proceededIds.size());
    }

    private void createIndex() {
        if (client.admin().indices().prepareExists(INDEX).execute().actionGet().isExists())
            return;

        client.admin().indices().prepareCreate(INDEX).get();
    }

    private boolean isInIndex(Long userId) {
        if (proceededIds.contains(userId))
            return true;

        if (client.prepareGet(INDEX, TYPE, userId.toString()).get().isExists()) {
            proceededIds.add(userId);
            return true;
        }

        return false;
    }

    public static void main(String[] args) throws IOException {
        App app = new App();
        app.start();
    }

    private List<Long> generateBatch() {
        ArrayList<Long> batch = new ArrayList<>();
        int batchSize = (idsToProceed.size() < BATCH_MAX_SIZE) ? idsToProceed.size() : BATCH_MAX_SIZE;

        for (int i = 0; i < batchSize; i++) {
            batch.add(idsToProceed.poll());
        }

        return batch;
    }



    private Map<Long, JSONObject> loadUsersBatch(final List<Long> batch) {
        JSONObject json = parseString(Http.getUsersInfo(batch));
        JSONObject response = (JSONObject) json.get("response");
        JSONArray users = (JSONArray) response.get("players");

        Map<Long, JSONObject> id2JSON = new HashMap<>(users.size());
        Iterator<JSONObject> iterator = users.iterator();
        while (iterator.hasNext()) {
            JSONObject user = iterator.next();
            Long id = Long.valueOf((String) user.get("steamid"));
            id2JSON.put(id, user);
        }

        return id2JSON;
    }

    private void loadUserGameStats(final Long userId, final JSONObject userJson) {
        JSONObject responseJson = parseString(Http.getUserGameStats(userId));

        userJson.put("hasCsGo", responseJson != null);
        if (responseJson == null)
            return;

        JSONObject playerStats = (JSONObject) responseJson.get("playerstats");
        JSONArray stats = (JSONArray) playerStats.get("stats");

        if (stats == null)
            return;

        Iterator<JSONObject> iterator = stats.iterator();
        while (iterator.hasNext()) {
            JSONObject stat = iterator.next();
            String name = (String) stat.get("name");
            Long value = (Long) stat.get("value");
            userJson.put(name, value);
        }
    }

    private void loadUserFriends(final Long userId, final JSONObject userJson) {
        JSONObject responseJson = parseString(Http.getUsersFriends(userId));

        userJson.put("isFriendListOpen", responseJson != null);
        if (responseJson == null)
            return;

        JSONObject friendsList = (JSONObject) responseJson.get("friendslist");
        JSONArray friends = (JSONArray) friendsList.get("friends");

        if (friends == null)
            return;

        userJson.put("numberOfFriends", friends.size());

        Iterator<JSONObject> iterator = friends.iterator();
        while (iterator.hasNext()) {
            JSONObject friend = iterator.next();
            Long id = Long.valueOf((String) friend.get("steamid"));
            idsToProceed.add(id);
        }
    }

    public static JSONObject parseString(final String json) {
        if (json == null)
            return null;

        try {
            return (JSONObject) jsonParser.parse(json);
        } catch (ParseException e) {
            e.printStackTrace();
            System.err.println("Couldn't parse JSON!");
        }
        return null;
    }
}
