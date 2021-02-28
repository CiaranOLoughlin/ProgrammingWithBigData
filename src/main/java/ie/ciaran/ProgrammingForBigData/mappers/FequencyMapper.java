package ie.ciaran.ProgrammingForBigData.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class FequencyMapper extends Mapper<Object, Text, Text, LongWritable> {
    private Text word = new Text();

    /*
    Very simple mapper, takes what was already outputed from LetterReducer, and remaps it for the next reducer.
    Needs to be here as you cant have 0 mappers in MapReduce process(Unless you use the identity mapper,
    which would basically to the same thing, but I wanted to define my own mapper for this. )
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] letters = value.toString().split("\t");
        for(int i=0; i<letters.length; i++) {
            Text text = new Text();
            //Map The key (ie. the letter value)
            text.set(letters[i]);
            LongWritable longWritable = new LongWritable();
            //Map the value(Ie. the total number of letters from the the original text). Ugly-ish, but works very well.
            i++;
            longWritable.set(Long.valueOf(letters[i]));
            context.write(text, longWritable);
        }
    }
}
