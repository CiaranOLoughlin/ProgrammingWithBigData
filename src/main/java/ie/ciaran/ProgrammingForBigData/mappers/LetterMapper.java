package ie.ciaran.ProgrammingForBigData.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class LetterMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public static ArrayList specialCharachters = new ArrayList(Arrays.asList(".",
            ",",
            "!" ,
            "\"" ,
            "#" ,
            "$" ,
            "%" ,
            "'" ,
            "(" ,
            ")" ,
            "*" ,
            "+" ,
            "-" ,
            "." ,
            "/" ,
            "0" ,
            "1" ,
            "2" ,
            "3" ,
            "4" ,
            "5" ,
            "6" ,
            "7" ,
            "8" ,
            "9" ,
            ":" ,
            ";" ,
            "?",
            "[",
            "]",
            "_",
            "—" ,
            "‘" ,
            "’" ,
            "“" ,
            "”" ,
            "\uFEFF",
            " "));

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] letters = value.toString().split("");
        for(int i=0; i<letters.length; i++) {
            if(!specialCharachters.contains(letters[i])) {
                Text text = new Text();
                text.set(letters[i]);
                context.write(text, one);
            }
        }
    }
}
