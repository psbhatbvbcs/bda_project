import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeywordsInRegionsReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueRegions = new HashSet<>();
        for (Text val : values) {
            uniqueRegions.add(val.toString());
        }
        if (uniqueRegions.size() > 3) {
            context.write(key, new Text("Regions Count: " + uniqueRegions.size()));
        }
    }
}
