import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopRegionsReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        PriorityQueue<String[]> queue = new PriorityQueue<>((a, b) -> Integer.parseInt(b[1]) - Integer.parseInt(a[1]));

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            queue.offer(new String[] { parts[0], parts[1] });
        }

        StringBuilder topRegions = new StringBuilder();
        int count = 0;
        while (!queue.isEmpty() && count < 3) {
            String[] regionData = queue.poll();
            topRegions.append(regionData[0]).append("(").append(regionData[1]).append("), ");
            count++;
        }

        context.write(key, new Text(topRegions.toString()));
    }
}
