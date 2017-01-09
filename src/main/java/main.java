import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class main {

    static MongoDatabase db;

    public static void main(String args[]) {
        connect();
        dropCollections();
        prepareCollections();
        File file = new File("src/main/resources/smallfile.txt");
        readFile(file);
    }

    private static void connect() {
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        db = mongoClient.getDatabase("db");
        System.out.println("Connect to database successfully");
    }

    private static void prepareCollections() {
        // ValidationOptions collOptions = new ValidationOptions().validationAction(ValidationAction.WARN).validator(Filters.exists("username"));
        //  ,new CreateCollectionOptions().validationOptions(collOptions)
        db.createCollection("user");
        db.createCollection("subreddit");
        db.createCollection("comment");
    }

    private static void dropCollections() {
        db.getCollection("user").drop();
        db.getCollection("subreddit").drop();
        db.getCollection("comment").drop();
    }

    private static void readFile(File file) {
        System.out.println("Importing data");
        Long startime = System.nanoTime();

        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        try {

            int l = 0;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = bufferedReader.readLine();
            while (line != null) {
                PostEntry obj = mapper.readValue(line, PostEntry.class);
                MongoCollection userCollection = db.getCollection("user");
                MongoCollection subredditCollection = db.getCollection("subreddit");


                //insert user and get the ObjectID mongo generates. This ID is used in comment collection to reference the user.
                //If user already exists, grab its id and use it as reference in comments.
                Document user = new Document("author", obj.author);
                FindIterable<Document> iterable = userCollection.find(user);
                ObjectId userId;

                if (iterable.first() != null) {      //Exist already
                    System.out.println("User already exist.");
                    userId = (ObjectId) iterable.first().get("_id");
                } else {                            //Didnt exist already
                    userCollection.insertOne(user);
                    userId = (ObjectId) user.get("_id");
                }

                //Same deal with subreddit
                Document subreddit = new Document("subreddit_name", obj.subreddit);
                FindIterable<Document> subIterable = subredditCollection.find(subreddit);
                ObjectId subredditId;

                if(subIterable.first() != null) {
                    System.out.println("Sub already exist.");
                    subredditId = (ObjectId) subIterable.first().get("_id");
                } else {
                    db.getCollection("subreddit").insertOne(subreddit);
                   subredditId = (ObjectId) subreddit.get("_id");
                }


                Document comment = new Document("author", userId)
                        .append("subreddit_id", subredditId)
                        .append("link_id", obj.link_id)
                        .append("body", obj.body)
                        .append("score", obj.score)
                        .append("created_utc", obj.created_utc);
                db.getCollection("comment").insertOne(comment);

                l++;
                line = bufferedReader.readLine();//Next line
            }


            System.out.println(l + " lines imported, time: " + (System.nanoTime() - startime) / 1000000 + "ms");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


