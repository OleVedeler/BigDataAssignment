package LinkCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HadoopLinkCount {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(HadoopLinkCount.class);
        job.setInputFormatClass(XmlInputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,args[0]);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }

    private static class Map extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            byte[] startTag = "[[".getBytes("utf-8");
            byte[] endTag = "]]".getBytes("utf-8");

            while (index <= value.getBytes().length - 1) {
                DataOutputBuffer buf = new DataOutputBuffer();
                if (readUntilMatch(startTag, value, buf, false)) {
                    try {
                        if (readUntilMatch(endTag, value, buf, true)) {
                            DataOutputBuffer tmpBuf = new DataOutputBuffer();
                            tmpBuf.write(buf.getData(), 0, buf.getLength() - endTag.length);
                            context.write(new Text(tmpBuf.getData()), new IntWritable(1));
                        }
                    } finally {
                        buf.reset();
                    }
                }
            }
        }
        int index = 0;
        private boolean readUntilMatch(byte[] match, Text values, DataOutputBuffer buf, boolean withinBlock) throws IOException {

            int i = 0;
            while(index <= values.getBytes().length - 1){
                int b = values.getBytes()[index];
                index++;
                if(withinBlock){
                    buf.write(b);
                }
                if (b == match[i]){
                    i++;
                    if(i >= match.length){
                        return true;
                    }
                }else{
                    i = 0;
                }
            }

            return false;
        }
    }

    private static class Reduce extends Reducer<Text,IntWritable, LongWritable, Text>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            for (IntWritable i : values) {
                sum+=i.get();
            }
            context.write(new LongWritable(sum), key);
        }
    }
}
