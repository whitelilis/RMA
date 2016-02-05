import com.admaster.data.metrics.core.AdMonitor;
import com.admaster.data.metrics.core.Reporter;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liuzhe on 16/2/2.
 */

public class RMAna implements TailerListener {

    public static Reporter reporter = AdMonitor.getReporter("1");
    public static HashMap<String, ReportData> cache = new HashMap<>();
    public static HashMap<String, Long> userUsed = new HashMap<>();
    public static HashMap<String, Long> queueUserd = new HashMap<>();
    public String startTime;


    private void addUse(HashMap<String, Long> aim, String name, long increase){
        if(aim.containsKey(name)){
            aim.put(name, aim.get(name) + increase);
        }else{
            aim.put(name, increase);
        }
    }

    public void ensureJobExist(String jobId){
        if(!cache.containsKey(jobId)){
            cache.put(jobId, new ReportData());
        }
    }

    public void init(Tailer tailer) {
        this.startTime = ReportData.df.format(new Date());
    }

    public void fileNotFound() {
        System.out.println("not fount");

    }

    public void fileRotated() {
        System.out.println("roll");

    }

    public void handle(String line) {
        String time = line.substring(0, ReportData.logTimeFormat.length());
        if(line.contains("FSSchedulerNode: Assigned container container")){
            //start task
            Pattern p = Pattern.compile("(.*)container container_((\\d+_\\d+).*) of capacity");
            Matcher m = p.matcher(line);
            m.find();
            String jobId = m.group(3);
            String taskId = m.group(2);
            ensureJobExist(jobId);
            cache.get(jobId).addTaskStarted(taskId, time);
        }else if(line.contains("FSSchedulerNode: Released container container")){
            //end task
            Pattern p = Pattern.compile("(.*)container container_((\\d+_\\d+).*) of capacity <memory:(\\d+),");
            Matcher m = p.matcher(line);
            m.find();
            String jobId = m.group(3);
            String taskId = m.group(2);
            ensureJobExist(jobId);
            long memMB = Long.decode(m.group(4));
            cache.get(jobId).addTaskFinished(taskId, time, memMB);
        }else {
            if (line.contains("RMAppManager$ApplicationSummary: appId=")) {
                //job finished
                Pattern p = Pattern.compile("(.*)application_(\\d+_\\d+),name=(.+),user=(\\w+),queue=([^,]+),.*finalStatus=(\\w+)");
                Matcher m = p.matcher(line);
                m.find();

                HashMap<String, String> aim = new HashMap<>();
                String jobId = m.group(2);
                String user = m.group(4);
                String queue = m.group(5);

                aim.put("jobId", jobId);
                aim.put("jobName", m.group(3));
                aim.put("user", user);
                aim.put("queue", queue);
                aim.put("status", m.group(6));


                ensureJobExist(jobId);

                ReportData toReport = cache.get(jobId);
                HashMap<String, String> r = toReport.prepareForReport(aim);
                toReport.show();

                addUse(userUsed, user, toReport.allMemMBxSeconds);
                addUse(queueUserd, queue, toReport.allMemMBxSeconds);
                try {
                    PiePng.makeJPG("user.jpg", String.format("From %s By user", startTime), userUsed);
                    PiePng.makeJPG("queue.jpg", String.format("From %s By Queue",startTime), queueUserd);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                reporter.add("rmLogAnalyze", toReport.result);
                reporter.report();
                // clear mem
                cache.remove(jobId);
            } else {// don't care
            }
        }
    }

    public void handle(Exception ex) {
        System.out.println("got " + ex);
    }


    public static void main(String[] args) {
        String filePath = "/Users/liuzhe/yarn-yarn-resourcemanager-1.master.adh.log";
        if(args.length > 0){
            filePath = args[0];
        }
        Tailer tailer = new Tailer(new File(filePath), new RMAna());
        tailer.run();
    }
}
