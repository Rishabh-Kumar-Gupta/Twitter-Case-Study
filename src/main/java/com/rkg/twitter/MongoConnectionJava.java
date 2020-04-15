package com.rkg.twitter;

import com.mongodb.*;
import com.mongodb.util.JSON;

public class MongoConnectionJava {
    String host;
    int port;
    String dataBase;
    String collectionName;
    MongoClient mongoClient;
    DB db;
    DBCollection dbCollection;

    /**
     * Particular made for MongoDB
     * @param host : local in case of local machine otherwise provide url here.
     * @param port : port number ex - 127.0.0.1:21407, 21407 is port number.
     * @param dataBase : DataBase
     * @param collectionName : Inside the database, select collection
     */
    MongoConnectionJava(String host,int port,String dataBase, String collectionName){
        this.host = host;
        this.port = port;
        this.dataBase = dataBase;
        this.collectionName = collectionName;
        mongoClient = new MongoClient(host, port);
        db = mongoClient.getDB(dataBase);
        dbCollection = db.getCollection(collectionName);
    }

    /**
     *
     * @param msg : Data which is to be inserted into collection.
     * @return : if Data inserted successfully, display as "Inserted"
     */
    String insertSingleData(String msg){
        try {
            dbCollection.insert((DBObject) JSON.parse(msg));
        }
        catch (Exception e){
            System.out.println("Skipped");
        }
        return "inserted";
    }


    String insertBulkData(){

        return "";
    }
}
