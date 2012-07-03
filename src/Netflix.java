import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Netflix extends Configured implements Tool {
    private static final String jobName = "netflix";

    private static final Log LOG = LogFactory.getLog(Netflix.class);

    public static final DateFormat ratingDateFormat =
        new SimpleDateFormat("yyyy-MM-dd");


    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = new Job(conf, jobName);
        job.setJarByClass(Netflix.class);

        if (args.length < 2) {
            usage();
        }

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setInputFormatClass(FileNameTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Netflix(), args);

        System.exit(res);
    }

    private void usage() {
        System.err.println("Usage: hadoop jar netflix.jar Netflix <in directory> <out directory>");
        System.exit(1);
    }
}
