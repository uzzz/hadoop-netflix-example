import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;

public class FileNameTextInputFormat extends FileInputFormat<Text, Text> {

    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec =
            new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }

    public RecordReader<Text, Text> createRecordReader(InputSplit genericSplit,
        TaskAttemptContext context) throws IOException {

        context.setStatus(genericSplit.toString());
        return new FileNameLineRecordReader(context.getConfiguration());
    }

}
