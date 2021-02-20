package ie.ciaran.ProgrammingForBigData;

import ie.ciaran.ProgrammingForBigData.service.HadoopRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMain {
    private static Logger log;
    private static HadoopRunnerService hadoopRunnerService;

    public static void init() {
        log = LoggerFactory.getLogger(ApplicationMain.class);
        hadoopRunnerService = new HadoopRunnerService();
    }

    public static void main(String[] args) throws Exception {
        init();
        log.info("Starting main application");
        hadoopRunnerService.startJobs();
        log.info("Returned from hadoop job succesfully");
    }
}
