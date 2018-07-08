/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sundar.es.crud.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import org.apache.log4j.Logger;

/**
 *
 * @author sundar
 * @since 2017-10-29
 * @modified 2017-10-29
 */
public class ElasticSearchClient {

    private static final Logger log = Logger.getLogger(ElasticSearchClient.class);
    private TransportClient client = null;
    private Properties elasticPro = null;

    public ElasticSearchClient() {
        try {
            elasticPro = new Properties();
            elasticPro.load(ElasticSearchClient.class.getResourceAsStream(ElasticSearchConstants.ELASTIC_PROPERTIES));
            log.info(elasticPro.getProperty("host"));
        } catch (IOException ex) {
            log.info("Exception occurred while load elastic properties : " + ex, ex);
        }
    }

    public TransportClient getInstant() {
        if (client == null) {
            client = getElasticClient();
        }
        return client;
    }

    private TransportClient getElasticClient() {
        try {

            Settings setting = Settings.builder()
                    .put("cluster.name", elasticPro.getProperty("cluster"))
                    .put("client.transport.sniff", Boolean.valueOf(elasticPro.getProperty("transport.sniff"))).build();

            // un-command this, if you have multiple node
//            TransportClient client1 = new PreBuiltTransportClient(setting)
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host1"), 9300))
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host1"), 9300));
            client = new PreBuiltTransportClient(setting)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticPro.getProperty("host")), Integer.valueOf(elasticPro.getProperty("port"))));
        } catch (UnknownHostException ex) {
            log.error("Exception occurred while getting Client : " + ex, ex);
        }
        return client;
    }

    public void closeClient(TransportClient client) {
        if (client != null) {
            client.close();
        }
    }
}
