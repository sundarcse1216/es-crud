/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sundar.es.crud;

/**
 *
 * @author sundar
 * @since 2017-10-29
 * @modified 2017-11-07
 */
public interface ElasticSearchCrud {

    public void CreateDocument();

    public void getDocument();

    public void updateDocument();

    public void deleteDocument();

    public void getMultipleDocument();

    public void insertMultipleDocument();

    public void searchDocument();

}
