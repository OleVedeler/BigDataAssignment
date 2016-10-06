package PageRank;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class HadoopRankCalc {

    private Job job = Job.getInstance(new Configuration());
    public HadoopRankCalc(String inputPath, String outputPath) throws Exception {

        job.setJarByClass(HadoopRankCalc.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

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

    private static class Map extends Mapper<LongWritable, Text,Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int pageIndex = value.find("\t");
            int rankIndex = value.find("\t", pageIndex + 1);

            // if it's -1 it could not find any links to it, and it's a read link
            if(rankIndex == -1 ) return;


            String page = Text.decode(value.getBytes(), 0, pageIndex);
            String PageAndRank = Text.decode(value.getBytes(), 0, rankIndex + 1);

            String allLinks = Text.decode(value.getBytes(), rankIndex, value.getBytes().length - rankIndex);

            String[] parsedLinks = allLinks.split(",");
            context.write(new Text(page), new Text("!"));

            for (String link : parsedLinks){
                context.write(new Text(link), new Text(PageAndRank + parsedLinks.length));
            }

            context.write(new Text(page), new Text("|" + allLinks));

        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, Text>{

        final float damping = 0.85f;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sumPageRank = 0.f;
            String links = new String();
            boolean isExsistingWikiPage = false;
            int inLinkCount = 0;

            for(Text value : values){
                if(value.toString().charAt(0) == '|'){

                    links = value.toString().substring(1);
                    continue;
                }
                if(value.toString().equals("!")) {
                    isExsistingWikiPage = true;
                    continue;
                }

                inLinkCount++;
                String[] informationSplit = value.toString().split("\t");
                float rank = Float.parseFloat(informationSplit[1]);
                int count = Integer.parseInt(informationSplit[2]);

                sumPageRank += (rank/(float)count);
            }
            if(!isExsistingWikiPage) return;

            String[] outLink = links.split(",");

            int outLinkCount = outLink.length;
            // Calculations
            float newPageRank = sumPageRank * damping + (1 - damping);

            context.write(key, new Text(inLinkCount + "\t" + outLinkCount + "\t" + newPageRank));
        }
    }
}
