/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sundar.es.crud.impl;

import com.sundar.es.crud.SearchAPIExample;
import com.sundar.es.crud.utils.ElasticSearchClient;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 *
 * @author sundar
 * @since 2017-11-01
 * @modified 2017-11-01
 */
public class SearchAPIExampleImpl implements SearchAPIExample {

    private static final Logger log = Logger.getLogger(SearchAPIExampleImpl.class);
    private final ElasticSearchClient ESclient = null;
    private TransportClient client = null;

    /**
     * This method used to get the document using scroll concept
     */
    @Override
    public void getDocumentUsingScroll() {
        try {
            client = ESclient.getInstant();
            QueryBuilder qb = termQuery("name", "sundar");
            SearchResponse scrollResp = client.prepareSearch("school", "college")
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                    .setScroll(new TimeValue(60000))
                    .setQuery(qb)
                    .setSize(100).get(); //max of 100 hits will be returned for each scroll, Scroll until no hits are returned
            do {
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    //Handle the hit...
                    hit.getField("name");
                }

                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while (scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
        } catch (Exception ex) {
            log.info("Exception occurred while Scroll Document : " + ex);
        }
    }

    /**
     * This method Search the document
     */
    @Override
    public void searchAll() {
        SearchHits hits = null;
        try {
            client = ESclient.getInstant();
            SearchResponse response = client.prepareSearch("school", "college")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setExplain(true)
                    .execute()
                    .actionGet();
            log.info("SearchResponse : " + response);
        } catch (Exception ex) {
            log.error("Exception occurred while Search All : " + ex, ex);
        }
    }

}
