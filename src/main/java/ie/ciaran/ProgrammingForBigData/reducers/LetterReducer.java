package ie.ciaran.ProgrammingForBigData.reducers;

import ie.ciaran.ProgrammingForBigData.domain.UpdateCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LetterReducer extends Reducer<Text, LongWritable, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        /*
        Iterate through each key value pair and sum all values, eg [(a, 1),(b,1),(a,1)] will go to [(a, 2),(b,1)]
         */
        for (LongWritable val : values) {
            sum += val.get();
        }
        /*
        Update the counter with total number of characters. Used when calculating frequencies
         */
        context.getCounter(UpdateCount.CNT).increment(sum);
        result.set(String.valueOf(sum));
        /*
        Write out the key value pair to the file system
         */
        context.write(key, result);
    }
}
