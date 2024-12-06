import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKeywordMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length == 10 && !fields[0].equals("country_code")) {
            String region = fields[1];
            String keyword = fields[2];
            String percentGain = fields[8];
            context.write(new Text(region), new Text(keyword + ":" + percentGain));
        }
    }
}