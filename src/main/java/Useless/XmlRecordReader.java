package Useless;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class XmlRecordReader extends RecordReader {
    private long start;
    private long end;
    private FSDataInputStream fsDataInputStream;
    private DataOutputBuffer buf = new DataOutputBuffer();
    private LongWritable currentKey;
    private Text currentValue;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        FileSystem fs = fileSplit.getPath().getFileSystem(context.getConfiguration());
        fsDataInputStream = fs.open(fileSplit.getPath());
        fsDataInputStream.seek(start);
    }


    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        long total = end - start;
        long read = fsDataInputStream.getPos() - start;

        return (float)read / (float)total;
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(fsDataInputStream.getPos() >= end) return false;
        byte[] startTag = "<page>".getBytes("utf-8");
        byte[] endTag = "</page>".getBytes("utf-8");

        if(readUntilMatch(startTag, false)){
            try {
                if(readUntilMatch(endTag,true)) {

                    currentKey = new LongWritable(fsDataInputStream.getPos());
                    currentValue = new Text();
                    currentValue.set(buf.getData(), 0, buf.getLength() - endTag.length);
                    return true;
                }
            }finally {
                buf.reset();
            }
        }
        return false;
    }


    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {

        int i = 0;
        while(true){
            int b = fsDataInputStream.read();
            if(b == -1){
                return false;
            }
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

            if(!withinBlock && fsDataInputStream.getPos() >= end){
                return false;
            }
        }
    }


    @Override
    public void close() throws IOException {
        fsDataInputStream.close();
    }
}
