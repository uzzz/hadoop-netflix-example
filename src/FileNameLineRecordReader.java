import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class FileNameLineRecordReader extends RecordReader<Text, Text> {
    private static final Log LOG = LogFactory.getLog(FileNameLineRecordReader.class);

    private final LineRecordReader lineRecordReader;

    private Text innerValue;

    private Text key;

    private Text value;

    private Path splitPath;

    public Class getKeyClass() { return Text.class; }

    public FileNameLineRecordReader(Configuration conf)
        throws IOException {

        lineRecordReader = new LineRecordReader();
    }

    public void initialize(InputSplit genericSplit,
        TaskAttemptContext context) throws IOException {
        splitPath = ((FileSplit) genericSplit).getPath();
        lineRecordReader.initialize(genericSplit, context);
    }

    public static void setKeyValue(Text key, Text value,
            Text line, Path path) {
        key.set(path.getName());
        value.set(line);
    }

    /** Read key/value pair in a line. */
    public synchronized boolean nextKeyValue() throws IOException {
        String line;
        if (lineRecordReader.nextKeyValue()) {
            innerValue = lineRecordReader.getCurrentValue();
            line = innerValue.toString();
        } else {
            return false;
        }
        if (line == null)
            return false;
        if (key == null) {
            key = new Text();
        }
        if (value == null) {
            value = new Text();
        }
        setKeyValue(key, value, innerValue, splitPath);
        return true;
    }

    public Text getCurrentKey() {
        return key;
    }

    public Text getCurrentValue() {
        return value;
    }

    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }

    public synchronized void close() throws IOException {
        lineRecordReader.close();
    }
}

