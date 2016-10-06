package UniqueWords;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class HadoopUniqueWords {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(HadoopUniqueWords.class);
        job.setInputFormatClass(XmlInputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,args[0]);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }

    private static class Map extends Mapper<LongWritable, Text,Text, IntWritable>{
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





            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    private static class Reduce extends Reducer<Text,IntWritable, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            values.iterator().next();
            if(!values.iterator().hasNext()){
                context.write(key, null);
            }
        }
    }

}
