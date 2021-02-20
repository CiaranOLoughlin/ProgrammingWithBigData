package ie.ciaran.ProgrammingForBigData.reducers;

import ie.ciaran.ProgrammingForBigData.domain.UpdateCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class FrequencyReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Text result = new Text();
    private final String tabCharachter = "\t";

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        String floatResult = getLetterFrequency(sum, getTotalNumberOfLetters(context));
        String value = getLanguageSelection(context) + tabCharachter + floatResult;
        result.set(value);
        context.write(key, result);

    }

    private String getLetterFrequency(Integer value, Integer totalNumberOfLetters){
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(8);
        Float result = (float)value/(float)totalNumberOfLetters;
        return df.format(result);
    }

    private String getLanguageSelection(Context context) {
        Configuration conf = context.getConfiguration();
        String currentLanguage = "";

        if(conf.get("pbg.letters.language") != null) {
            currentLanguage = conf.get("pbg.letters.language");
        } else {
            currentLanguage = "Woops";
        }
        return currentLanguage;
    }

    private Integer getTotalNumberOfLetters(Context context) {
        Configuration conf = context.getConfiguration();
        Integer totalNumberOfLetters = 0;

        if(conf.get("pbg.letters.total") != null) {
            totalNumberOfLetters = Integer.valueOf(conf.get("pbg.letters.total"));
        } else {
            totalNumberOfLetters = 1;
        }
        return totalNumberOfLetters;
    }
}
