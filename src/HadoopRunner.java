import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopRunner {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Big Data Analysis");

        switch (args[2]) {
            case "topKeyword":
                job.setJarByClass(HadoopRunner.class);
                job.setMapperClass(TopKeywordMapper.class);
                job.setReducerClass(TopKeywordReducer.class);
                break;
            case "avgPercentGain":
                job.setJarByClass(HadoopRunner.class);
                job.setMapperClass(AvgPercentGainMapper.class);
                job.setReducerClass(AvgPercentGainReducer.class);
                break;
            case "highestRank":
                job.setJarByClass(HadoopRunner.class);
                job.setMapperClass(HighestRankMapper.class);
                job.setReducerClass(HighestRankReducer.class);
                break;
            case "topRegions":
                job.setJarByClass(HadoopRunner.class);
                job.setMapperClass(TopRegionsMapper.class);
                job.setReducerClass(TopRegionsReducer.class);
                break;
            case "keywordsInRegions":
                job.setJarByClass(HadoopRunner.class);
                job.setMapperClass(KeywordsInRegionsMapper.class);
                job.setReducerClass(KeywordsInRegionsReducer.class);
                break;
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
