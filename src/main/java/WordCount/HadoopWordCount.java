package WordCount;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        import java.io.IOException;
        import java.util.ArrayList;


public class HadoopWordCount {


    public static void main(String[] args) throws Exception {


        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(HadoopWordCount.class);

        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

        // Mapper class
    private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            /* Stores the value to a string array. Then we manipulate the data to make sure the results from our program is
             * as accurate as possible. We have done our best to remove everything that is not part of the text body,
             * which is what we want */

            final String[] words = value.toString()

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


            // This for-loop writes the word in the "words" array with a preceding count of one.

            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));

            }
        }
    }

    /* We have decided to alter the structure of the output from the reducer to display the count in the first column
    and the word in the second. This makes the output more readable and is achieved by switching the placement of the Reducers
     output key and value. We also have to change setOutputKeyClass and setOutputValueClass accordingly!
     */
        // Reducer class
    private static class Reduce extends Reducer<Text, IntWritable, LongWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // This is the actual counter. We define a long variable (sum) to store the count for each key (word) while it iterates

            long sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }

            // Writes the count of each word in the wikipedia dataset ---e.g.--->  123    alphabet
            context.write(new LongWritable(sum), key);
        }
    }
}
