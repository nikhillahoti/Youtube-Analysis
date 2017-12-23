package youtube;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.StringTokenizer;

public class MRmapper  extends Mapper <LongWritable,Text,LongWritable,Text> {
    static String IFS=",";
    static String OFS=",";
    static int NF=11;
    static int badrecordcounter = 0;
    static int counterr = 0;

    public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {

        /** USvideos.csv
        video_id
        title
        channel_title
        category_id
        tags
        views
        likes
        dislikes
        comment_total
        thumbnail_link
        date
        */

    	// TODO 1: remove schema line
    	if (key.get() == 0) {
    		return;
    	}

    	// TODO 2: convert value to string
    	StringTokenizer strKey = new StringTokenizer(value.toString(), IFS);

        // TODO 3: count num fields, increment bad record counter, and return if bad
    	if (strKey.countTokens() != NF) {
    		badrecordcounter++;
    		return;
    	}

        // TODO 4: pull out fields of interest and TODO 5: construct key and composite value
       // Video ID
    	String strVideoID = strKey.nextToken();

        // Title
        strKey.nextToken();

        // Channel Title
        strKey.nextToken();

        // Category ID
        String strCategoryID = strKey.nextToken();

        // Tags
        strKey.nextToken();

        // TODO 5: construct key and composite value
    	String strkey = strVideoID + "," + strKey.nextToken() + "," + strKey.nextToken() + "," + strKey.nextToken(); // 3 tokens are likes,dislikes and views

    	// Comment total
    	strKey.nextToken();

    	// Thumb Nail
    	strkey = strkey + "," + strKey.nextToken();

        // TODO 6: write key value pair to context
        LongWritable lngKey = new LongWritable();
        lngKey.set(Long.valueOf(strCategoryID));
        context.write(lngKey, new Text(strkey));
    }
}
