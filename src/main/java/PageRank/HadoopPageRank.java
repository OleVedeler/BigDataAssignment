package PageRank;
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

public class HadoopPageRank {

    public static void main(String[] args) throws Exception {
        //Job one
        HadoopXmlParser xmlParser = new HadoopXmlParser(args[0], "output/1/" + args[1]);
        xmlParser.runJob();

        //Job two
        HadoopRankCalc rankCalc = new HadoopRankCalc("output/1/" + args[1], "output/2/" + args[1]);
        rankCalc.runJob();
    }
}
