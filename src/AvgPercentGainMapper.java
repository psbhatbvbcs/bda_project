import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgPercentGainMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length == 10 && !fields[0].equals("country_code")) {
            String region = fields[1];
            int percentGain = Integer.parseInt(fields[8]);
            context.write(new Text(region), new IntWritable(percentGain));
        }
    }
}
