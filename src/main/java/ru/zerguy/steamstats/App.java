package ru.zerguy.steamstats;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class App {

    public static final Long MY_STEAM_ID = 76561198023043869L;

    public static final JSONParser jsonParser = new JSONParser();

    public static final int BATCH_MAX_SIZE = 100;

    public static void main(String[] args) throws IOException {
        App app = new App();
        app.start();
    }

    private void start() {
        TransportClient client = createConnection();
        if (client == null)
            return;

        System.out.println("Connection created");

        Queue<Long> idsToProceed = new LinkedList<>();
        Set<Long> proceededIds = new HashSet<>();

        idsToProceed.add(MY_STEAM_ID);

        while (!idsToProceed.isEmpty()) {
            List<Long> batch = generateBatch(idsToProceed);
            Map<Long, JSONObject> id2JSON = loadUsersBatch(batch);

            for (Long userId : batch) {
                if (proceededIds.contains(userId))
                    continue;

                proceededIds.add(userId);
                System.out.println("proceeding " + userId);

                JSONObject userJson = id2JSON.get(userId);
                loadUserFriends(userId, userJson, idsToProceed);
                loadUserGameStats(userId, userJson);

                IndexResponse response = client.prepareIndex("steam", "player").setSource(userJson.toString()).get();
                System.out.println("added      " + userId);
                System.out.println("Users proceeded: " + proceededIds.size());
            }

            if (proceededIds.size() > 100)
                break;
        }

        System.out.println("Closing connection");
        client.close();
    }

    private List<Long> generateBatch(final Queue<Long> idsToProceed) {
        ArrayList<Long> batch = new ArrayList<>();
        int batchSize = (idsToProceed.size() < BATCH_MAX_SIZE) ? idsToProceed.size() : BATCH_MAX_SIZE;

        for (int i = 0; i < batchSize; i++) {
            batch.add(idsToProceed.poll());
        }

        return batch;
    }

    private TransportClient createConnection() {
        System.out.println("Creating connection");

        try {
            return new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.err.println("Couldn't connect to ES");
            return null;
        }
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

        userJson.put("hasCsGo", userJson != null);
        if(responseJson == null)
            return;

        JSONObject playerStats = (JSONObject) responseJson.get("playerstats");
        JSONArray stats = (JSONArray) playerStats.get("stats");

        Iterator<JSONObject> iterator = stats.iterator();
        while (iterator.hasNext()) {
            JSONObject stat = iterator.next();
            String name = (String) stat.get("name");
            String value = (String) stat.get("value");
            userJson.put(name, value);
        }

        userJson.put("stats", stats);
    }

    private void loadUserFriends(final Long userId, final JSONObject userJson, final Queue<Long> idsToProceed) {
        JSONObject responseJson = parseString(Http.getUsersFriends(userId));

        userJson.put("isFriendListOpen", userJson != null);
        if(responseJson == null)
            return;

        JSONObject friendsList = (JSONObject) responseJson.get("friendslist");
        JSONArray friends = (JSONArray) friendsList.get("friends");

        userJson.put("numberOfFriends", friends.size());

        Iterator<JSONObject> iterator = friends.iterator();
        while (iterator.hasNext()) {
            JSONObject friend = iterator.next();
            Long id = Long.valueOf((String) friend.get("steamid"));
            idsToProceed.add(id);
        }
    }

    private JSONObject parseString(final String json) {
        if(json == null)
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
