package youtube;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class MRreducer  extends Reducer <LongWritable,Text,Text,Text> {
    public static String IFS=",";
    public static String OFS=",";
   public void reduce(LongWritable key, Iterable<Text> values, Context context)
		   throws IOException, InterruptedException {

       // TODO 1: initialize variables
       int maxlikes = 0, maxviews = 0, maxdislikes = 0;

       String Category_id="";
       String video_id_likes ="", thumb_nail_likes = "";
       String video_id_dislikes ="", thumb_nail_dislikes = "";
       String video_id_views ="", thumb_nail_views = "";

       // TODO 2: loop through values to find most viewed, most liked, and most disliked video
       for(Text itr : values) {
    	   String[] tokens = itr.toString().split(",");
    	   String currVideoID = tokens[0].toString();
    	   int currViews = Integer.parseInt(tokens[1].toString());
    	   int currlikes = Integer.parseInt(tokens[2].toString());
    	   int currdislikes = Integer.parseInt(tokens[3].toString());
    	   String currthumbnail = tokens[4].toString();

    	   if (currlikes > maxlikes)
    	   {
    		   video_id_likes = currVideoID;
    		   thumb_nail_likes = currthumbnail;
    		   maxlikes = currlikes;
    	   }

    	   if (currdislikes > maxdislikes)
    	   {
    		   video_id_dislikes = currVideoID;
    		   thumb_nail_dislikes = currthumbnail;
    		   maxdislikes = currdislikes;
    	   }

    	   if (currViews > maxviews)
    	   {
    		   video_id_views = currVideoID;
    		   thumb_nail_views = currthumbnail;
    		   maxviews = currViews;
    	   }
       }

       // TODO 3: write the key-value pair to the context exactly as defined in lab write-up
       context.write(new Text("category_id: "), new Text(key.toString()));
       context.write(new Text("most views: "),new Text(video_id_views + "," + thumb_nail_views));
       context.write(new Text("most likes: "),new Text(video_id_likes + "," + thumb_nail_likes));
       context.write(new Text("most dislikes: "),new Text(video_id_dislikes + "," + thumb_nail_dislikes));
   }
}
