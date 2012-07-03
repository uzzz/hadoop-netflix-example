import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(ReduceClass.class);

    public void reduce(Text key, Iterable<Text> values,
                       Context context) throws IOException,
                                               InterruptedException {
        double sumFirstTwoWeeks = 0;
        int countFirstTwoWeeks = 0;
        double sumOverall = 0;
        int countOverall = 0;

        for(Text rating:values) {
            String[] tokens = rating.toString().split(",");
            int ratingValue = Integer.parseInt(tokens[1]);
            sumOverall   += ratingValue;
            countOverall += 1;
            try {
                Date movieReleaseDate = Netflix.ratingDateFormat.parse(tokens[0]);
                Date ratingDate = Netflix.ratingDateFormat.parse(tokens[2]);

                if (isLessThanTwoWeeks(movieReleaseDate, ratingDate)) {
                    sumFirstTwoWeeks   += ratingValue;
                    countFirstTwoWeeks += 1;
                }
            } catch (ParseException e ) {
                LOG.error("Unable to parse rating date");
            }
        }

        double avgOverall       = sumOverall / countOverall;
        double avgFirstTwoWeeks = sumFirstTwoWeeks / countFirstTwoWeeks;

        context.write(key, new Text(avgFirstTwoWeeks + "\t" + avgOverall));
    }

    private boolean isLessThanTwoWeeks(Date d1, Date d2) {
        long milliseconds = Math.abs(d1.getTime() - d2.getTime());
        return TimeUnit.MILLISECONDS.toDays(milliseconds) <= 14;
    }
}
