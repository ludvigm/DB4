import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DBCollectionDistinctOptions;
import org.bson.BSONObject;
import org.bson.Document;
import org.bson.types.ObjectId;

import javax.management.Query;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;


public class main {

    static MongoDatabase db;
    static MongoCollection userCollection;
    static MongoCollection subredditCollection;
    static MongoCollection commentCollection;


    public static void main(String args[]) {
        connect();
        //dropCollections();
       // prepareCollections();

        userCollection = db.getCollection("user");
        subredditCollection = db.getCollection("subreddit");
        commentCollection = db.getCollection("comment");

        File file = new File("src/main/resources/RC_2007-10");
        //importData(file);
        getNumberOfCommentsForUser("HiggsBoson");
        getNumberOfCommentsPerDayToSub("politics");
        getNumberOfCommentsContainingText("lol");
        q4("t3_5yba3");
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
        DBCollectionDistinctOptions opts = new DBCollectionDistinctOptions();
        db.createCollection("subreddit");
        db.createCollection("comment");
    }

    private static void dropCollections() {
        db.getCollection("user").drop();
        db.getCollection("subreddit").drop();
        db.getCollection("comment").drop();
    }

    private static void importData(File file) {
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

                //insert user and get the ObjectID mongo generates. This ID is used in comment collection to reference the user.
                //If user already exists, grab its id and use it as reference in comments.
                Document user = new Document("author", obj.author);
                FindIterable<Document> iterable = userCollection.find(user);
                ObjectId userId;

                if (iterable.first() != null) {      //Exist already
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
                    subredditId = (ObjectId) subIterable.first().get("_id");
                } else {
                    subredditCollection.insertOne(subreddit);
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


    //1. How many comments have a specific user posted?
    private static void getNumberOfCommentsForUser(String user) {

        //First we look up the user in user collection, and get the ObjectID for the user.
        //This ObjectID is used in comment table to reference to the user.
        Document userToFind = new Document("author", user);
        ObjectId userID = (ObjectId) ((Document) userCollection.find(userToFind).first()).get("_id");

        //Use objectID we found here to get all comments the user has posted.
        Document commentToFind = new Document("author", userID);
        long count = commentCollection.count(commentToFind);

        System.out.println("User \""+user+"\" has posted " + count + " comments.");
    }


    //2. How many comments does a specific subreddit get per day?
    private static void getNumberOfCommentsPerDayToSub(String subreddit) {
        //Lookup subreddit in subreddit collection
        Document subToFind = new Document("subreddit_name", subreddit);
        ObjectId subID = (ObjectId) ((Document) subredditCollection.find(subToFind).first()).get("_id");

        Document commentToFind = new Document("subreddit_id",subID);
        System.out.println("Comments to sub " + subreddit+ ":"+ commentCollection.count(commentToFind));


    }

    //3. How many comments include the word ‘lol’?
    private static void getNumberOfCommentsContainingText(String text) {
        Document commentBodyToFind = new Document("body", Pattern.compile(Pattern.quote(text)));

        long count = commentCollection.count(commentBodyToFind);
        System.out.println(count + " comments contains the word \"" + text + "\"");
    }

    //4. Users that commented on a specific link has also posted to which subreddits?
    private static void q4(String link_id) {
        Document doc = new Document("link_id", link_id);

        commentCollection.find(doc);

    }

}


