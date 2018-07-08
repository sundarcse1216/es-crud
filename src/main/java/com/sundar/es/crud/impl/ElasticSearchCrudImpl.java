/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sundar.es.crud.impl;

import com.sundar.es.crud.ElasticSearchCrud;
import com.sundar.es.crud.utils.ElasticSearchClient;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

/**
 *
 * @author sundar
 * @since 2017-10-29
 * @modified 2017-11-09
 */
public class ElasticSearchCrudImpl implements ElasticSearchCrud {

    private static final Logger log = Logger.getLogger(ElasticSearchCrudImpl.class);
    private ElasticSearchClient ESclient = null;
    private TransportClient client = null;

    public ElasticSearchCrudImpl() {
        ESclient = new ElasticSearchClient();
    }

    /**
     * This method Create the Index and insert the document(s)
     */
    @Override
    public void CreateDocument() {

        try {
            client = ESclient.getInstant();
            IndexResponse response = client.prepareIndex("school", "tenth", "1")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("name", "Sundar")
                            .endObject()
                    ).get();
            if (response != null) {
                String _index = response.getIndex();
                String _type = response.getType();
                String _id = response.getId();
                long _version = response.getVersion();
                RestStatus status = response.status();
                log.info("Index has been created successfully with Index: " + _index + " / Type: " + _type + "ID: " + _id);
            }
        } catch (IOException ex) {
            log.error("Exception occurred while Insert Index : " + ex, ex);
        }
    }

    /**
     * This method get the matched document
     */
    @Override
    public void getDocument() {
        try {
            client = ESclient.getInstant();
            GetResponse response = client.prepareGet("school", "tenth", "1")
                    .setOperationThreaded(false)
                    .get();
            if (response != null) {
                Map<String, GetField> FieldsMap = response.getFields();
                log.info("Response Data : " + FieldsMap.toString());
            }
        } catch (Exception ex) {
            log.error("Exception occurred while get Document : " + ex, ex);
        }
    }

    /**
     * This method delete the matched Document
     */
    @Override
    public void deleteDocument() {
        try {
            client = ESclient.getInstant();
            DeleteResponse deleteResponse = client.prepareDelete("school", "tenth", "1")
                    //                    .setOperationThreaded(false)
                    .get();
            if (deleteResponse != null) {
                deleteResponse.status();
                deleteResponse.toString();
                log.info("Document has been deleted...");
            }
        } catch (Exception ex) {
            log.error("Exception occurred while delete Document : " + ex, ex);
        }
    }

    /**
     * This method updated the matched Document
     */
    @Override
    public void updateDocument() {
        try {
            client = ESclient.getInstant();
            UpdateRequest updateReguest = new UpdateRequest("school", "tenth", "1")
                    .doc(jsonBuilder()
                            .startObject()
                            .field("name", "Sundar S")
                            .endObject());
            UpdateResponse updateResponse = client.update(updateReguest).get();
            if (updateResponse != null) {
                updateResponse.getGetResult();
                log.info("Index has been updated successfully...");
            }
        } catch (IOException | InterruptedException | ExecutionException ex) {
            log.error("Exception occurred while update Document : " + ex, ex);
        }
    }

    /**
     * This method get the multiple Documents
     */
    @Override
    public void getMultipleDocument() {
        try {
            client = ESclient.getInstant();
            MultiGetResponse multipleItems = client.prepareMultiGet()
                    .add("school", "tenth", "1")
                    .add("school", "nineth", "1", "2", "3", "4")
                    .add("college", "be", "1")
                    .get();

            multipleItems.forEach(multipleItem -> {
                GetResponse response = multipleItem.getResponse();
                if (response.isExists()) {
                    String json = response.getSourceAsString();
                    log.info("Respense Data : " + json);
                }
            });
        } catch (Exception ex) {
            log.error("Exception occurred while get Multiple Document : " + ex, ex);
        }
    }

    /**
     * This method insert the more than one Document at a time
     */
    @Override
    public void insertMultipleDocument() {
        try {
            client = ESclient.getInstant();
            BulkRequestBuilder bulkDocument = client.prepareBulk();

            // either use client#prepare, or use Requests# to directly build index/delete requests
            bulkDocument.add(client.prepareIndex("school", "tenth", "1")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("name", "Sundar")
                            .field("age", "23")
                            .field("gender", "Male")
                            .endObject()
                    )
            );

            bulkDocument.add(client.prepareIndex("school", "tenth", "2")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("name", "Sundar S")
                            .field("age", "23")
                            .field("gender", "Male")
                            .endObject()
                    )
            );
            BulkResponse bulkResponse = bulkDocument.get();
            if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
            } else {
                log.info("All Documents inserted successfully...");
            }

        } catch (IOException ex) {
            log.error("Exception occurred while get Multiple Document : " + ex, ex);
        }
    }

    /**
     * This method Search the available Document
     */
    @Override
    public void searchDocument() {
        SearchHits hits = null;
        try {
            client = ESclient.getInstant();
            SearchResponse response = client.prepareSearch("school", "college")
                    .setTypes("tenth", "be")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.termQuery("name", "sundar"))
                    .setPostFilter(QueryBuilders.rangeQuery("age").from(15).to(24))
                    .setFrom(0).setSize(60).setExplain(true)
                    .get();

            if (response != null) {
                hits = response.getHits();
            }
            if (hits != null) {

                while (hits.iterator().hasNext()) {
                    hits.iterator().next();
                }
            }
        } catch (Exception ex) {
            log.error("Excption occurred while Search Document : " + ex);
        }
    }
}
