import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgPercentGainReducer extends Reducer<Text, IntWritable, Text, Text> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0, count = 0;
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }
        double avg = (double) sum / count;
        context.write(key, new Text("Avg Percent Gain: " + avg));
    }
}
