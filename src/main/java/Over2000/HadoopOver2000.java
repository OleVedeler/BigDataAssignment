package Over2000;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HadoopOver2000 {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(HadoopOver2000.class);

        job.setInputFormatClass(XmlInputFormat.class);


        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,args[0]);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }

    private static class Map extends Mapper<LongWritable,Text,LongWritable,LongWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            final String[] words = value.toString().toLowerCase()
                    .replaceAll("\\|(.+?)]]", "]]")                 //Removes everything between sqr brackets (
                    .replaceAll("\\{\\{.*?}}", " ")                 //Removes double curly brackets and everything in-between
                    .replaceAll("\\{\\|.*?\\|}", " ")               //---||--- curly brackets encapsulating pipes
                    .replaceAll("&lt;.*?&gt;"," ")                  //---||--- &lt; and &gt;
                    .replaceAll("<timestamp>.*?</timestamp>", " ")  //---||--- the pages timestamp and its value
                    .replaceAll("<ns>.*?</title>", " ")             //---||--- ns and title
                    .replaceAll("<id>.*?</id>", " ")
                    .replaceAll("<sha1>.*?</sha1>", " ")
                    .replaceAll("(<title>|</title>|<ns>|wikt:|</ns>|" +                 // removes all metadata
                            "<revision>|&amp;|<id>|</id>|" +
                            "<timestamp>|</timestamp>|<contributor>|" +
                            "<username>|</username>|</contributor>" +
                            "|<parentid>|</parentid>|<comment>|</comment>" +
                            "|&lt;ref|&lt;|&gt;|/ref|&lt;/ref&gt;&lt;ref&gt\\;)", " ")
                    .toLowerCase().replaceAll("\\[\\[file:.*?]]", " ")
                    .replaceAll("\\[\\[image:.*?]]]]", " ")
                    .replaceAll("\\[\\[category:.*?]]", " ")
                    .replaceAll("\\[\\[special:.*?]]", " ")
                    .replaceAll("\\[\\[s:.*?]]", " ")
                    .replaceAll("\\[http:.*?]", " ")
                    .replaceAll("<parentid>.*?</parentid>", " ")
                    .replaceAll("(<contributor>|</contributor>|/ref|br|&quot;)", " ")
                    .replaceAll("[']{2,3}", " ").replaceAll("(\\|url.*?html)", " ")
                    .replaceAll("[^a-z']+", " ").split("\\s+");

            int wordCount = 0;
            for(String word: words){
                wordCount++;
                if(wordCount > 2000){
                    context.write(key, new LongWritable(1));
                    break;
                }
            }
        }
    }

    private static class Reduce extends Reducer<LongWritable, LongWritable, Text, LongWritable>{
        int sum = 0;

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            for (LongWritable i : values){
                sum += i.get();
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Number of articles with over 2000 words:"), new LongWritable(sum));
        }
    }
}
