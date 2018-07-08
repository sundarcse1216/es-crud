/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sundar.es;

import com.sundar.es.crud.ElasticSearchCrud;
import com.sundar.es.crud.SearchAPIExample;
import com.sundar.es.crud.impl.ElasticSearchCrudImpl;
import com.sundar.es.crud.impl.SearchAPIExampleImpl;
import com.sundar.es.crud.utils.ElasticSearchConstants;
import org.apache.log4j.Logger;

/**
 *
 * @author sundar
 * @since 2017-10-29
 * @modified 2017-10-29
 */
public class ESTester {

    private static final Logger log = Logger.getLogger(ESTester.class);

    /**
     * This is constructor, validate the user given arguments
     *
     * @param args
     */
    public ESTester(String[] args) {
        boolean valid = true;
        for (String arg : args) {
            if (Integer.valueOf(arg) > 9) {
                valid = false;
            }
        }
        if (valid) {
            startProcess(args);
        } else {
            log.info(ElasticSearchConstants.INVALID_MSG);
        }

    }

    /**
     * Start the operation(s) based on input
     *
     * @param args - operation ID, which is given by user
     */
    private void startProcess(String[] args) {
        for (String arg : args) {
            if (Integer.valueOf(arg) > 9) {
                log.info(ElasticSearchConstants.INVALID_MSG);
                break;
            } else {
                ElasticSearchCrud esCRUD = new ElasticSearchCrudImpl();
                SearchAPIExample esSearch = new SearchAPIExampleImpl();
                log.info("--------------------------------------");
                switch (Integer.valueOf(arg)) {
                    case 1:
                        log.info("Read Process is started...");
                        log.info("--------------------------------------");
                        esCRUD.getDocument();
                        break;
                    case 2:
                        log.info("Write Process is started...");
                        log.info("--------------------------------------");
                        esCRUD.CreateDocument();
                        break;
                    case 3:
                        log.info("Update Process is started...");
                        log.info("--------------------------------------");
                        esCRUD.updateDocument();
                        break;
                    case 4:
                        log.info("Delete Process is started...");
                        log.info("--------------------------------------");
                        esCRUD.deleteDocument();
                        break;
                    case 5:
                        log.info("Read Multiple Process is started...");
                        log.info("--------------------------------------");
                        esCRUD.getMultipleDocument();
                        break;
                    case 6:
                        log.info("Insert Multiple Process is started...");
                        log.info("--------------------------------------");
                        esCRUD.insertMultipleDocument();
                        break;
                    case 7:
                        log.info("Search Process is started...");
                        log.info("--------------------------------------");
                        esCRUD.searchDocument();
                        break;
                    case 8:
                        log.info("Search All Process is started...");
                        log.info("--------------------------------------");
                        esSearch.searchAll();
                        break;
                    case 9:
                        log.info("Read Using Scroll Process is started...");
                        log.info("--------------------------------------");
                        esSearch.getDocumentUsingScroll();
                        break;
                    case 10:
                        log.info("Get Specific fields is started...");
                        log.info("--------------------------------------");
                        esSearch.getSpecificFields();
                        break;
                    case 11:
                        log.info("Read Using Must Query is started...");
                        log.info("--------------------------------------");
                        esSearch.SearchByMustQuery();
                        break;
                    case 12:
                        log.info("Read Using Should Query is started...");
                        log.info("--------------------------------------");
                        esSearch.SearchBySouldQuery();
                        break;
                    default:
                        log.info("Invalid Argument : " + arg);
                        log.info("--------------------------------------");
                        break;
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            log.info("Welcome to ElasticSearch CRUD Operations!!!");
            if (args.length > 0 && args.length <= 9) {
                ESTester esTester = new ESTester(args);
            } else {
                log.info("At least one argument, at most four argument is Required, (" + args.length + " given)");
            }
        } catch (Exception e) {
            log.error("Exception occurred : " + e, e);
        }
    }
}
