import com.sun.org.apache.bcel.internal.generic.LoadClass;
import com.sun.tools.doclets.formats.html.SourceToHTMLConverter;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;

import java.io.*;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liuzhe on 16/2/2.
 */

public class RMAna implements TailerListener {

    //public static final Reporter reporter = AdMonitor.getReporter("1");
    public static final HashMap<String, ReportData> cache = new HashMap<>();
    public static final HashMap<String, Long> userUsed = new HashMap<>();
    public static final HashMap<String, Long> queueUsed = new HashMap<>();
    public String startTime = null;
    public static long userAll = 0;
    public static long queueAll = 0;



    public void replay(String logDir) throws IOException {
        File dir = new File(logDir);
        final String logPrefix = "report.log";
        if(dir.exists() && dir.isDirectory()){
            File[] logs = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.startsWith(logPrefix) && ! logPrefix.equals(name);
                }
            });
            Arrays.sort(logs);
            for(File f : logs){
                replayOneFile(f);
            }

            File lastLog = new File(logDir + "/" + logPrefix);
            if(lastLog.exists() && lastLog.isFile()){
                replayOneFile(lastLog);
            }
        }
    }

    private void replayOneFile(File f) throws IOException {
        System.out.println("process " + f.getName());
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = br.readLine();
        while(line != null){
            // 2016-02-14 00:01:44,608 INFO ReportData: 1453215494036_495810##track_report##root.track.igrp##3342336##SUCCEEDED##Pi
            if(line.length() > ReportData.logTimeFormat.length() + 3 && line.charAt(4) == '-'){
                if(this.startTime == null){
                    this.startTime = line.substring(0, ReportData.logTimeFormat.length());
                }else {
                }
                String[] parts = line.split(" ");
                if(parts.length >= 4) {
                    String[] needs = parts[4].split("##");
                    userAll += Long.decode(needs[3]);
                    queueAll += Long.decode(needs[3]);
                    addUse(userUsed, needs[1], Long.decode(needs[3]));
                    addUse(queueUsed, needs[2], Long.decode(needs[3]));
                }else{
                    System.err.println("length error :" + line);
                }
            }
            line = br.readLine();
        }
        br.close();
    }

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
        try {
            replay("daily");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void fileNotFound() {
        System.out.println("not found");

    }

    public void fileRotated() {
    }

    public void handle(String line) {
        if(line.length() < ReportData.logTimeFormat.length() + 3){
            return;
        }else {
            String time = line.substring(0, ReportData.logTimeFormat.length());
            if (line.contains("SchedulerNode: Assigned container container")) {
                //start task
                Pattern p = Pattern.compile("(.*)container container_((\\d+_\\d+).*) of capacity");
                Matcher m = p.matcher(line);
                m.find();
                String jobId = m.group(3);
                String taskId = m.group(2);
                ensureJobExist(jobId);
                cache.get(jobId).addTaskStarted(taskId, time);
            } else if (line.contains("SchedulerNode: Released container container")) {
                //end task
                Pattern p = Pattern.compile("(.*)container container_((\\d+_\\d+).*) of capacity <memory:(\\d+),");
                Matcher m = p.matcher(line);
                m.find();
                String jobId = m.group(3);
                String taskId = m.group(2);
                ensureJobExist(jobId);
                long memMB = Long.decode(m.group(4));
                cache.get(jobId).addTaskFinished(taskId, time, memMB);
            } else {
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
                    toReport.prepareForReport(aim);
                    toReport.show();

                    addUse(userUsed, user, toReport.allMemMBxSeconds);
                    userAll += toReport.allMemMBxSeconds;
                    addUse(queueUsed, queue, toReport.allMemMBxSeconds);
                    queueAll += toReport.allMemMBxSeconds;
                    try {
                        String now = ReportData.df.format(new Date());
                        PiePng.makeJPG("user.jpg", String.format("By user %s -> %s : %d", startTime, now, userAll), userUsed);
                        PiePng.makeJPG("queue.jpg", String.format("By queue %s -> %s : %d", startTime, now, queueAll), queueUsed);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    //reporter.add("rmLogAnalyze", toReport.result);
                    //reporter.report();
                    // clear mem
                    cache.remove(jobId);
                } else {// don't care
                }
            }
        }
    }

    public void handle(Exception ex) {
        ex.printStackTrace();
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
