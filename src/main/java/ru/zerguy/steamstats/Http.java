package ru.zerguy.steamstats;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

public class Http {

    private static final String KEY = "18A4FFCA6F72D1EB21FE386E521D1695";
    private static final String API_ENDPOINT = "http://api.steampowered.com/";

    private static final int CSGO_ID = 730;
    private static final int SLEEP_TIME = 400;

    private static long timeStart = System.currentTimeMillis();
    private static long numberOFRequests = 0;
    private static long timeOfPrevRequest = timeStart;

    public static String getUsersInfo(List<Long> steamIds) {
        return getResponse(generatePlayerSummariesUrl(steamIds));
    }

    private static String getResponse(final String url) {
        HttpResponse response = sendGet(url);

        if (getStatusCode(response) != 200)
            return null;

        try {
            return getBody(response);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Couldn't get body from response");
            return null;
        }
    }

    public static String getUsersFriends(final Long user) {
        return getResponse(generateFriendListUrl(user));
    }

    public static String getUserGameStats(final Long user) {
        return getResponse(generateGameStatsUrl(user));
    }

    private static HttpResponse sendGet(String url) {
        makePauseBetweenRequests();

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);

        try {
            return client.execute(request);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void makePauseBetweenRequests() {
        long timeCurrent = System.currentTimeMillis();
        long timeElapsed = timeCurrent - timeOfPrevRequest;
        numberOFRequests++;
        double averageTimeBetweenRequests = (timeCurrent - timeStart) / numberOFRequests;

        System.out.print("Elapsed time: " + (double) timeElapsed / 1000 + ";   ");
        System.out.println("Avg time between requests: " + averageTimeBetweenRequests / 1000);


        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        timeOfPrevRequest = System.currentTimeMillis();
    }

    private static int getStatusCode(HttpResponse response) {
        return response.getStatusLine().getStatusCode();
    }

    private static String getBody(HttpResponse response) throws IOException {
        InputStream content = response.getEntity().getContent();

        BufferedReader rd = new BufferedReader(new InputStreamReader(content));

        StringBuilder result = new StringBuilder();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line.trim());
        }

        return result.toString();
    }

    private static String generatePlayerSummariesUrl(List<Long> steamIds) {
        return API_ENDPOINT + "ISteamUser/GetPlayerSummaries/v0002/?key=" + KEY +"&steamids=["
                + steamIds.stream().map(Object::toString).collect(Collectors.joining(","))
                + "]";
    }

    private static String generateFriendListUrl(long steamId) {
        return API_ENDPOINT + "ISteamUser/GetFriendList/v0001/?key=" + KEY + "&steamid=" + steamId + "&relationship=friend";
    }

    private static String generateGameStatsUrl(long steamId) {
        return API_ENDPOINT + "ISteamUserStats/GetUserStatsForGame/v0002/?appid=" + CSGO_ID + "&key=" + KEY + "&steamid=" + steamId;
    }
}
