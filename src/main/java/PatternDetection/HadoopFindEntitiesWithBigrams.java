package PatternDetection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class HadoopFindEntitiesWithBigrams {

    public HadoopFindEntitiesWithBigrams(String in, String out) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(HadoopFindEntitiesWithBigrams.class);
        job.setInputFormatClass(PatternDetection.XmlInputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,in);
        FileOutputFormat.setOutputPath(job,new Path(out));

        job.waitForCompletion(true);
    }

    private static class Map extends Mapper<LongWritable, Text,Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            index = 0;

            // Cleanup of text
            String tmpValue[] = value.toString().split("<text");
            String tmpValue1[] = tmpValue[1].split("==References==");
            String tmpValue2[] = tmpValue1[0].split("== References ==");

            String cleanValue = tmpValue2[0]
                    .replaceAll("\\|(.+?)]]", "]]")
                    .replaceAll("\\{\\{.*?}}", " ")
                    .replaceAll("\\{\\|.*?\\|}", " ")
                    .replaceAll("&lt;.*?&gt;"," ")
                    .replaceAll("<timestamp>.*?</timestamp>", " ")
                    .replaceAll("<ns>.*?</title>", " ")
                    .replaceAll("<id>.*?</id>", " ")
                    .replaceAll("(<title>|</title>|<ns>|wikt:|</ns>|<revision>|&amp;|<id>|</id>|" +
                            "<timestamp>|</timestamp>|<contributor>|<username>|</username>|</contributor>" +
                            "|<parentid>|</parentid>|<comment>|</comment>|&lt;ref|&lt;|&gt;|/ref|&lt;/ref&gt;&lt;ref&gt\\;)", " ")
                    .replaceAll("(===.*?===)", " ")
                    .replaceAll("(==.*?==)", " ")
                    .replaceAll("\\[\\[File:.*?]]", " ")
                    .replaceAll("\\[\\[Image:.*?]]]]", " ")
                    .replaceAll("\\[\\[Category:.*?]]", " ")
                    .replaceAll("\\[\\[Special:.*?]]", " ")
                    .replaceAll("\\[\\[s:.*?]]", " ")
                    .replaceAll("\\[http:.*?]", " ")
                    .replaceAll("<parentid>.*?</parentid>", " ")
                    .replaceAll("(<contributor>|</contributor>|/ref|br|&quot;)", " ")
                    .replaceAll("[']{2,3}", " ")
                    .replaceAll("(\\|url.*?html)", " ");
                   //.replaceAll("[^a-z']+", " ");

            //"?<=[.?!;])\s+(?=\p{Lu})

            // Find sentences
            String[] sentences = cleanValue.split("(?<=[.?!;])\\s+(?=\\p{Lu})");


            //Find entities in sentences
            for (String sentence : sentences ){
                ArrayList<Text> entities = new ArrayList<Text>();
                index = 0;

                while(index <= sentence.length()){
                    DataOutputBuffer buf = new DataOutputBuffer();
                    if(readUntilMatch("[[".getBytes(),new Text(sentence),buf,false)){
                        if (readUntilMatch("]]".getBytes(), new Text(sentence), buf, true)) {

                            // Remove entity from written data
                            DataOutputBuffer tmpBuf = new DataOutputBuffer();
                            tmpBuf.write(buf.getData(), 0, buf.getLength() - "]]".length());
                            entities.add(new Text(tmpBuf.getData()));
                        }
                    }
                }
                if(entities.size() >= 2){
                    for(Text entity : entities) {
                        context.write(entity, new Text(sentence));
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
                if(index >= values.getLength()) return false;

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

    private static class Reduce extends Reducer<Text,Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text value : values) {
                String bigrams = "";
                int firstEntityPos = value.find("[[" + key + "]]");
                int secondEntityStartPos = value.find("[[" , firstEntityPos + ("[[]]".getBytes().length + key.getLength()) );
                int secondEntityEndPos = value.find("]]" , secondEntityStartPos);

                // If there is no other entity
                // This is supposidly not possible because we check that there is at least two entities in each sentance
                // We only add this because the algorithme in the mapper is prone to failure.
                if(secondEntityStartPos == -1) return;

                String secondEntity = value.toString().substring(secondEntityStartPos, secondEntityEndPos);
                String[] words = value.toString().substring(firstEntityPos,secondEntityStartPos).split("\\s");


                boolean first = true;
                for (int i = 0; i < words.length - 1; i++) {
                    if(!first){
                        bigrams += ",";
                    }
                    bigrams += words[i] + " " + words[i+1];
                    first = false;

                }
                context.write(new Text(key + "|" + secondEntity), new Text(bigrams));
            }
        }
    }
}
