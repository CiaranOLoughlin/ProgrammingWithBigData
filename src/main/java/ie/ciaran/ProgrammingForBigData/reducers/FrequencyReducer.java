package ie.ciaran.ProgrammingForBigData.reducers;

import ie.ciaran.ProgrammingForBigData.domain.UpdateCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class FrequencyReducer extends Reducer<Text, LongWritable, Text, Text> {
    private Text result = new Text();
    private final String tabCharachter = "\t";
    private static final float minimumUsefulFrequency = 0.0001f;

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        /*
        Get the values of each letter value pair.
         */
        for (LongWritable val : values) {
            sum += val.get();
        }
        /*
        Get a string result of dividing letter total number by total number of letters
         */
        String floatResult = getLetterFrequency(sum, getTotalNumberOfLetters(context));
        if(!floatResult.equals("0")) {
             /*
            Creates the output string, so it becomes <letter><tab><language><tab><frequency>
             */
            String value = getLanguageSelection(context) + tabCharachter + floatResult;
            result.set(value);
            /*
            Write the result to the file.
             */
            context.write(key, result);
        }
    }

    /*
    Utility functions
     */

    /*
    Return a formatted string of the calculated letter frequency.
    Returning as a string removes the problematic prints of 2.3e8 or 0.00000000001 etc
     */
    private String getLetterFrequency(Integer value, Integer totalNumberOfLetters){
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(8);
        //Calculate frequency. TotalNumberOfSpecificLetter divided by TotalNumberOfLettersAltogether.
        Float result = (float)value/(float)totalNumberOfLetters;

        if (result < minimumUsefulFrequency) {
            return "0";
        }
        return df.format(result);
    }

    /*
    Reads the language variable passed in from the driver class(HadoopRunnerService)
     */
    private String getLanguageSelection(Context context) {
        Configuration conf = context.getConfiguration();
        String currentLanguage = "";

        if(conf.get("pbg.letters.language") != null) {
            currentLanguage = conf.get("pbg.letters.language");
        } else {
            /*
            Hopefully this never triggers, never any harm to check for nulls though.
             */
            currentLanguage = "Woops";
        }
        return currentLanguage;
    }

    /*
   Reads the language letter total variable passed in from the driver class(HadoopRunnerService)
    */
    private Integer getTotalNumberOfLetters(Context context) {
        Configuration conf = context.getConfiguration();
        Integer totalNumberOfLetters = 0;

        if(conf.get("pbg.letters.total") != null) {
            /*
            Context values have to be strings, otherwise we could avoid this casting, as it can be detrimental to performance to do this to often.
             */
            totalNumberOfLetters = Integer.valueOf(conf.get("pbg.letters.total"));
        } else {
            totalNumberOfLetters = 1;
        }
        return totalNumberOfLetters;
    }
}
