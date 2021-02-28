package ie.ciaran.ProgrammingForBigData.mappers;

import ie.ciaran.ProgrammingForBigData.domain.SpecialCharacters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LetterMapper extends Mapper<Object, Text, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        /*
        Split the input text by every charachter, and then loop through each one.
         */
        String[] letters = value.toString().split("");
        for(int i=0; i<letters.length; i++) {
            /*
            Checks if the current letter/character is a predefined special character. If it is, ignore it altogether.
             */
            if(!SpecialCharacters.specialCharacters.contains(letters[i])) {
                Text text = new Text();
                /*
                Capitalise all letters, so we have a frequency distributed soley by letters, rather than divided by capitals and small letters.
                Characters without an uppercase version will not be effected.
                 */
                text.set(letters[i].toUpperCase());
                /*
                Write the letter and the number as a pair ie. [(a, 1),(b,1),(a,1)]
                The reducer will pick this up and sum it together.
                 */
                context.write(text, one);
            }
        }
    }
}
