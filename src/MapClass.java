import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

class MapClass extends Mapper<Text, Text, Text, Text> {
    static class MovieInfo {
        public String title;
        public String released;

        public MovieInfo(String title, String released) {
            this.title = title;
            this.released = released;
        }
    };

    private static final Log LOG = LogFactory.getLog(MapClass.class);

    private static final DateFormat moviesDf = new SimpleDateFormat("dd MMM yyyy");

    private Hashtable<Integer, MovieInfo> joinData =
        new Hashtable<Integer, MovieInfo>();

    private Text val = new Text();

    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String strValue = value.toString();

        if (!strValue.endsWith(":")) {
            String[] fields   = strValue.split(",");
            String rating     = fields[1];
            String ratingDate = fields[2];
            MovieInfo info    = joinData.get(movieIdFromFileName(key));

            if (info != null) {
                val.set(info.released + "," + rating + "," + ratingDate);
                context.write(new Text(info.title), val);
            }
        }
    }

    private Integer movieIdFromFileName(Text fileName) {
        String strKey = fileName.toString();
        return Integer.valueOf(strKey.replaceAll("mv_0+", "").
                replaceAll(".txt", ""));
    }

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        Calendar calendar  = Calendar.getInstance();

        try {
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
            if (cacheFiles != null && cacheFiles.length > 0) {
                String line;
                String[] tokens;
                BufferedReader joinReader = new BufferedReader(
                        new FileReader(cacheFiles[0].toString()));
                try {
                    while ((line = joinReader.readLine()) != null) {
                        tokens = line.split(",");
                        try {
                            Date releaseDate = moviesDf.parse(tokens[1]);
                            calendar.setTime(releaseDate);
                            if (calendar.get(Calendar.YEAR) >= 2000) {
                                MovieInfo info = new MovieInfo(tokens[2],
                                                Netflix.ratingDateFormat.format(releaseDate));
                                joinData.put(new Integer(tokens[0]), info);
                            }
                        } catch (ParseException e) {}
                    }
                } finally {
                    joinReader.close();
                }
            }
        } catch (IOException e) {
            System.out.println("Exception reading distributed cache: " + e);
        }
    }
}
