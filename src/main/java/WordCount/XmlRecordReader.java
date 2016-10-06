/* The main purpose of the record reader is to breaks the data into key/value pairs for input to the Mapper. */

package WordCount;

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

    /* Declare the variables, including the splits start and end position, the input stream,
       our output buffer and the current key and value. */
    private long start;
    private long end;
    private FSDataInputStream fsDataInputStream;
    /* The DataOutputBuffer is a reusable data output implementation that writes to an in-memory buffer.
       This saves memory over creating a new DataOutputStream and ByteArrayOutputStream each time data is written. */
    private DataOutputBuffer buf = new DataOutputBuffer();
    private LongWritable currentKey;
    private Text currentValue;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        // Declear a fileSplit variable and assign to it the input split
        FileSplit fileSplit = (FileSplit) split;

        // Set start to the position of the first byte in the file (filesplit) to process.
        start = fileSplit.getStart();

        // Defines the end of the file split: Set the end variable to start of process + the number of bytes in the file to process.
        end = start + fileSplit.getLength();

        // Returns a reference to an existing FileSystem and assigns its fileSplit path to the FileSystem variable fs.
        FileSystem fs = fileSplit.getPath().getFileSystem(context.getConfiguration());

        // Basically this defines the input stream and then we use the .seek method to find the given offset which in this case is "start".
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

        //  Total = Length of the file split.
        long total = end - start;

        /* Read = Current position in the input stream - the start position of the split.
           Tells us the current position in the input stream. */
        long read = fsDataInputStream.getPos() - start;
        return (float)read / (float)total;
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        /* First we check if the file split is valid. If the position is equal to or greater than
           the position of the end it will advance */
        if(fsDataInputStream.getPos() >= end) return false;

        /* Encodes this <page> and </page> strings into a sequence of bytes using the platform's default charset,
           storing the result into a new byte array. */
        byte[] startTag = "<page>".getBytes("utf-8");
        byte[] endTag = "</page>".getBytes("utf-8");


        /* Basically this block generate the position of specific keys and values from the current input stream
           based on different conditions regarding the start and end tags boolean status */
        if(readUntilMatch(startTag, false)){
            try {
                if(readUntilMatch(endTag,true)) {

                    currentKey = new LongWritable(fsDataInputStream.getPos());
                    currentValue = new Text();
                    currentValue.set(buf.getData(), 0, buf.getLength() - endTag.length);
                    return true;
                }
            }finally {
                // Resets the buffer for each run
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
