import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKeywordReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String topKeyword = "";
        int maxGain = Integer.MIN_VALUE;

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            String keyword = parts[0];
            int gain = Integer.parseInt(parts[1]);
            if (gain > maxGain) {
                maxGain = gain;
                topKeyword = keyword;
            }
        }
        context.write(key, new Text(topKeyword + ":" + maxGain));
    }
}
