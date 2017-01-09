/**
 * Created by johanrovala on 08/12/16.
 */
public class PostEntry {
    public String id;
    public String parent_id;
    public String link_id;
    public String name;
    public String author;
    public String body;
    public String subreddit_id;
    public String subreddit;
    public int score;
    public int created_utc;

    @Override
    public String toString() {
        return "id: " + id + " - body: " + body;
    }
}
