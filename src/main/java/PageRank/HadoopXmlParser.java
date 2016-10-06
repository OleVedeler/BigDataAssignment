package PageRank;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class HadoopXmlParser {

    private Job job = Job.getInstance(new Configuration());
    public HadoopXmlParser(String inputPath, String outputPath) throws Exception {

        job.setJarByClass(HadoopXmlParser.class);

        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

    }

    public void runJob() throws Exception{
        job.waitForCompletion(true);
    }

    private static class Map extends Mapper<LongWritable, Text,Text, Text>{
        int index = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            byte[] startTag = "[[".getBytes("utf-8");
            byte[] endTag = "]]".getBytes("utf-8");

            if(value.toString().contains("#REDIRECT")) return;

            index = 0;
            // TODO: handle if getTitle returns null
            Text title = new Text(getTitle(value));
            while (index <= value.getBytes().length - 1) {
                DataOutputBuffer buf = new DataOutputBuffer();

                if (readUntilMatch(startTag, value, buf, false)) {
                    try {
                        if (readUntilMatch(endTag, value, buf, true)) {
                            DataOutputBuffer tmpBuf = new DataOutputBuffer();
                            tmpBuf.write(buf.getData(), 0, buf.getLength() - endTag.length);

                            context.write(title, new Text(tmpBuf.getData()));

                        }
                    } finally {
                        buf.reset();
                    }
                }
            }
        }

        private boolean readUntilMatch(byte[] match, Text values, DataOutputBuffer buf, boolean withinBlock) throws IOException {

            int i = 0;
            int x = 0;
            while(index <= values.getBytes().length - 1){
                int b = values.getBytes()[index];
                index++;
                if(withinBlock){
                    //Stops writing if | is meet
                    //writes the match, because it gets removed in the code above
                    //
                    if(b == "|".getBytes()[0]){
                        buf.write(match);
                        return true;
                    }
                    buf.write(b);
                }

                if (b == "#REDIRECT".getBytes()[x]){
                    x++;
                    if(x >= match.length){
                        return false;
                    }
                }else{
                    x = 0;
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

        private byte[] getTitle(Text values) throws IOException {

            int i = 0;
            int pos = 0;
            byte[] startTag = "<title>".getBytes();
            byte[] endTag = "</title>".getBytes();
            byte[] currentTag = startTag;

            DataOutputBuffer buf = new DataOutputBuffer();

            while(pos <= values.getBytes().length - 1){
                int b = values.getBytes()[pos];
                pos++;

                if(currentTag == endTag){
                    buf.write(b);
                }

                if (b == currentTag[i]){
                    i++;
                    if(i >= currentTag.length){
                        if(currentTag == startTag){
                            currentTag = endTag;
                            i = 0;
                        }else{
                            //Remove </title> tag
                            DataOutputBuffer tmpBuf = new DataOutputBuffer();
                            tmpBuf.write(buf.getData(), 0, buf.getLength() - endTag.length);

                            return tmpBuf.getData();
                        }
                    }
                }else{
                    i = 0;
                }
            }
            return null;
        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String Links = "0.25\t";

            while (values.iterator().hasNext()) {
                Links += values.iterator().next().toString();
                if(values.iterator().hasNext()){
                    Links += ",";
                }
            }
            context.write(key, new Text(Links));
        }
    }
}
