package ie.ciaran.ProgrammingForBigData.reducers;

import ie.ciaran.ProgrammingForBigData.domain.UpdateCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LetterReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Text result = new Text();
    private final String tabCharachter = "\t";

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.getCounter(UpdateCount.CNT).increment(sum);
        String value = "" + sum;
        result.set(value);
        context.write(key, result);
    }
}
