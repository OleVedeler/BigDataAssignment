package PatternDetection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HadoopEntityWordCount {

    public HadoopEntityWordCount(String in, String out) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(HadoopEntityWordCount.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,in);
        FileOutputFormat.setOutputPath(job,new Path(out));

        job.waitForCompletion(true);
    }

    private static class Map extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                String[] informationSplit = value.toString().split("\t");
                String entities = informationSplit[0];
                String[] bigrams = informationSplit[1].split(",");
                for (int i = 0; i < bigrams.length; i++) {
                    context.write(new Text(entities + ":" + bigrams[i]), new IntWritable(1));
            }
        }
    }

    private static class Reduce extends Reducer<Text,IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable i : values) {
                sum+=i.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

}
