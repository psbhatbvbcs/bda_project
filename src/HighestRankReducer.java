import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestRankReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int minRank = Integer.MAX_VALUE;
        for (Text val : values) {
            int rank = Integer.parseInt(val.toString());
            if (rank < minRank) {
                minRank = rank;
            }
        }
        context.write(key, new Text("Highest Rank: " + minRank));
    }
}
