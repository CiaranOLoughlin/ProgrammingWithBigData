package ie.ciaran.ProgrammingForBigData.service;

import ie.ciaran.ProgrammingForBigData.domain.UpdateCount;
import ie.ciaran.ProgrammingForBigData.mappers.LetterMapper;
import ie.ciaran.ProgrammingForBigData.mappers.FequencyMapper;
import ie.ciaran.ProgrammingForBigData.reducers.LetterReducer;
import ie.ciaran.ProgrammingForBigData.reducers.FrequencyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;


public class HadoopRunnerService {
    private Logger log;
    private static final String letterSummerJob = "letterSummerJob-";
    private static final String frequencyDistributionJob = "frequencyDistributionJob-";
    private static final String inputFilePath = "BookResources";
    private static final String outputFilePath = "Output/HadoopOutput-";

    /*
    Set up and configure logging. Uses log4j.properties found in src/main/resources.
    WARNING, hadoop also uses this properties file, setting output to debug will result in huge amounts of logs!
     */
    public HadoopRunnerService() {
        log = LoggerFactory.getLogger(HadoopRunnerService.class);
        log.info("Starting up hadoop runner service class");
    }

    /*
      First Map Reduce job to run.
      This reads input from bookResources, and maps number of occurrences of letters in texts,
      Also returns total number of letters in all books, used to calculate frequency of letters in the following job.
    */
    private String startLetterSummerJob(Configuration conf, String language) throws Exception{
        log.info("Configuring Job 1, {}, for {} books",letterSummerJob, language);
        Path outputPath = new Path(outputFilePath + language);
        Path inputPath = new Path(inputFilePath + "/" + language);

        Job job = Job.getInstance(conf, letterSummerJob + language);
        /*
        Sets output file name, not essential just makes it easier to understand which output is relevant
         */
        job.getConfiguration().set("mapreduce.output.basename", letterSummerJob + language);

        Configuration chainConfig=new Configuration(false);
        ChainMapper.addMapper(job, LetterMapper.class, Object.class, Text.class, Text.class, LongWritable.class, chainConfig);

        Configuration reduceConf = new Configuration(false);
        ChainReducer.setReducer(job, LetterReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, reduceConf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
        log.info("Job 1, {} for language {} has been completed successfully",letterSummerJob, language);
        /*
        Returns the string value of the number of total number of letter found in all books in specific language.
         */
        return String.valueOf(job.getCounters().findCounter(UpdateCount.CNT).getValue());
    }

    /*
    Second Map reduce job.
    Calculates letter frequency. Reads output of the first job, takes the letter occurrence value and divides by total number of letters in language.
    Writes output in format of <letter> <language> <frequency>
     */
    private void startLetterFrequencyJob(Configuration conf, String language, String totalNumberOfLetters) throws Exception {
        log.info("Configuring Job 2, {}, for {} books", frequencyDistributionJob, language);
        Path outputPathFinal = new Path(outputFilePath + language + "-final");
        Path inputPath = new Path(outputFilePath + language);

        Job job = Job.getInstance(conf, frequencyDistributionJob + language);
        job.getConfiguration().set("mapreduce.output.basename", frequencyDistributionJob + language);

        Configuration chainConfig=new Configuration(false);
        ChainMapper.addMapper(job, FequencyMapper.class, Object.class, Text.class, Text.class, LongWritable.class, chainConfig);

        Configuration reduceConf = new Configuration(false);
        /*
        Uses config values to pass variables to reducer class.
        Language is first value passed in. Second value is total number of letters found in map reduce process.
         */
        reduceConf.set("pbg.letters.language", language);
        reduceConf.set("pbg.letters.total", totalNumberOfLetters);
        ChainReducer.setReducer(job, FrequencyReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, reduceConf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPathFinal);

        job.waitForCompletion(true);
        log.info("Job 2, {} for language {} has been completed successfully", frequencyDistributionJob, language);
    }

    private void configureJobs(String language){
        Configuration conf = new Configuration();
        try {
            String totalNumberOfLetters = startLetterSummerJob(conf, language);
            startLetterFrequencyJob(conf, language, totalNumberOfLetters);
        } catch (Exception ex) {
            log.error("Exception thrown when running hadoop job, exception details are: ", ex);
        }
    }

    public void startJobs() {
        /*
        Creates an arraay List of all the directories in the file path.
        Directories here should be named with the language of books in them.
        Example BookResources/English/[book1.txt, book2.txt]
         */
        File file = new File("./" + inputFilePath);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });

        /*
        Run each job sequentially.
        TODO Look at running asynchronously, could improve performance, could drive me mad. 50/50 there.
         */

        for(String directoryName : directories){
            configureJobs(directoryName);
        }
    }
}
