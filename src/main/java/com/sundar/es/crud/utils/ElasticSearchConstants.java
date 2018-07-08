/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sundar.es.crud.utils;

/**
 *
 * @author sundar
 */
public interface ElasticSearchConstants {

    public final String INVALID_MSG = "Invalid Argument(s) \n1 - Read / 2 - Write / 3 - Update / 4 - Delete "
            + "/ 5 - Read Multiple / 6 - Insert Multiple / 7 - Search / 8 - Search All / 9 - Read Using Scroll "
            + "/ 10 - Get Specifig Fields / 11 - Must Query / 12 - Should Query";
    public String ELASTIC_PROPERTIES = "/conf/elasticSearch.properties";
}
