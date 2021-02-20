package ie.ciaran.ProgrammingForBigData.mappers;

import ie.ciaran.ProgrammingForBigData.domain.SpecialCharacters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class LetterMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] letters = value.toString().split("");
        for(int i=0; i<letters.length; i++) {
            if(!SpecialCharacters.specialCharacters.contains(letters[i])) {
                Text text = new Text();
                text.set(letters[i].toUpperCase());
                context.write(text, one);
            }
        }
    }
}
