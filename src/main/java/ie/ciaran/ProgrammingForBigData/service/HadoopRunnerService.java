package ie.ciaran.ProgrammingForBigData.service;

import ie.ciaran.ProgrammingForBigData.domain.UpdateCount;
import ie.ciaran.ProgrammingForBigData.mappers.LetterMapper;
import ie.ciaran.ProgrammingForBigData.mappers.FequencyMapper;
import ie.ciaran.ProgrammingForBigData.reducers.LetterReducer;
import ie.ciaran.ProgrammingForBigData.reducers.FrequencyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

    public HadoopRunnerService() {
        log = LoggerFactory.getLogger(HadoopRunnerService.class);
        log.info("Starting up hadoop runner service class");
    }

    private String startLetterSummerJob(Configuration conf, String language, Path inputPath, Path outputPath) throws Exception{
        log.info("Configuring Job 1, {}, for {} books",letterSummerJob, language);
        Job job = Job.getInstance(conf, letterSummerJob + language);
        job.getConfiguration().set("mapreduce.output.basename", letterSummerJob + language);

        Configuration chainConfig=new Configuration(false);
        ChainMapper.addMapper(job, LetterMapper.class, Object.class, Text.class, Text.class, IntWritable.class, chainConfig);

        Configuration reduceConf = new Configuration(false);
        ChainReducer.setReducer(job, LetterReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, reduceConf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
        log.info("Job 1, {} for language {} has been completed successfully",letterSummerJob, language);
        return String.valueOf(job.getCounters().findCounter(UpdateCount.CNT).getValue());
    }

    private void startLetterFrequencyJob(Configuration conf, String language, Path inputPath, Path outputPath, String totalNumberOfLetters) throws Exception{
        log.info("Configuring Job 2, {}, for {} books", frequencyDistributionJob, language);
        Job job = Job.getInstance(conf, letterSummerJob + language);
        job.getConfiguration().set("mapreduce.output.basename", frequencyDistributionJob + language);

        Configuration chainConfig=new Configuration(false);
        ChainMapper.addMapper(job, FequencyMapper.class, Object.class, Text.class, Text.class, IntWritable.class, chainConfig);

        Configuration reduceConf = new Configuration(false);
        reduceConf.set("pbg.letters.language", language);
        reduceConf.set("pbg.letters.total", totalNumberOfLetters);
        ChainReducer.setReducer(job, FrequencyReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, reduceConf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPathFinal = new Path(outputFilePath + language + "-final");
        FileInputFormat.addInputPath(job, outputPath);
        FileOutputFormat.setOutputPath(job, outputPathFinal);

        job.waitForCompletion(true);
        log.info("Job 2, {} for language {} has been completed successfully", frequencyDistributionJob, language);
    }

    private void configureJobs(String language){
        Path inputPath = new Path(inputFilePath + "/" + language);
        Path outputPath = new Path(outputFilePath + language);
        Configuration conf = new Configuration();
        try {
            String totalNumberOfLetters = startLetterSummerJob(conf, language, inputPath, outputPath);
            startLetterFrequencyJob(conf, language, inputPath, outputPath, totalNumberOfLetters);
        } catch (Exception ex) {
            log.error("Exception thrown when running hadoop job, exception details are: ", ex);
        }
    }

    public void startJobs() {
        File file = new File("./" + inputFilePath);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });

        for(String directoryName : directories){
            configureJobs(directoryName);
        }
    }
}
