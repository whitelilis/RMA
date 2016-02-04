import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**i
 * Created by liuzhe on 16/2/3.
 */
public class ReportData{
    public HashMap<String, String> startedTasks = new HashMap<>(); // taskId : startTimeString
    public long allMemMBxSeconds = 0;
    public static final String logTimeFormat = "yyyy-MM-dd HH:mm:ss";
    public static SimpleDateFormat df = new SimpleDateFormat(logTimeFormat);
    public HashMap<String, String> result = new HashMap<>();

    public HashMap<String, String> prepareForReport(String jobId, String user, String queue){
        startedTasks.clear();
        result.put("jobId", jobId);
        result.put("user", user);
        result.put("queue", queue);
        result.put("memMBxSecond", String.format("%d", allMemMBxSeconds));
        return result;
    }

    public void show(){
        System.out.println(String.format("%s  %s:%s:%s:%s", df.format(new Date()), result.get("jobId"), result.get("user"), result.get("queue"), result.get("memMBxSecond")));
    }

    public static long secondsBetween(String start, String end){
        DateFormat df = new SimpleDateFormat(logTimeFormat);

        try {
            return (df.parse(end).getTime() - df.parse(start).getTime()) / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
            return 0L;
        }
    }

    public void addTaskStarted(String taskId, String startString){
        startedTasks.put(taskId, startString);
    }

    public void addTaskFinished(String taskId, String endString, long memMB){
        if(startedTasks.containsKey(taskId)){
            allMemMBxSeconds += memMB * secondsBetween(startedTasks.get(taskId), endString);
        }
    }
}
