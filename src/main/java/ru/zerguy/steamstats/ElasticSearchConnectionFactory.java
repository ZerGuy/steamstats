package ru.zerguy.steamstats;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ElasticSearchConnectionFactory {

    private static final int PORT = 9300;

    public static TransportClient createConnection() {
        System.out.println("Creating connection");

        try {
            return new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName("localhost"), PORT));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.err.println("Couldn't connect to ES");
            return null;
        }
    }
}
