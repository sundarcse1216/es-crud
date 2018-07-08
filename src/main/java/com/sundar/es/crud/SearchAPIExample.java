/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sundar.es.crud;

/**
 *
 * @author sundar
 * @since 2017-11-01
 * @modified 2017-11-01
 */
public interface SearchAPIExample {

    public void getDocumentUsingScroll();

    public void searchAll();
    
    public void getSpecificFields();
    
    public void SearchByMustQuery();
    
    public void SearchBySouldQuery();
    
    public void SearchByAgregationQuery();

}
