package ie.ciaran.ProgrammingForBigData.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class FequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] letters = value.toString().split("\t");
        for(int i=0; i<letters.length; i++) {
            Text text = new Text();
            text.set(letters[i]);
            IntWritable intWritable = new IntWritable();
            i++;
            intWritable.set(Integer.valueOf(letters[i]));
            context.write(text, intWritable);
        }
    }
}
