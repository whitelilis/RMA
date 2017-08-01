import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

public class MetricReporter {
    public HashMap<String, Metric> metrics = new HashMap<>();
    public Thread timer;

    public MetricReporter(){
        this.timer = new Thread(new DailyTimer(this));
        this.timer.start();
    }

    public void addMetric(String measurement, String parName, long mt){
        if(metrics.containsKey(parName)){
            metrics.get(parName).addMt(mt);
        }else{
            metrics.put(parName, new Metric(measurement, parName, mt));
        }
    }


    public void report(){
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        Collection<Metric> ms = metrics.values();
        int size = ms.size();
        int count = 0;
        for(Metric metric : ms){
            sb.append(metric.getString());
            count = count + 1;
            if(count < size){
                sb.append(",");
            }
        }
        sb.append("]");


        try {
            Helper.sendPost("http://17.slave.adh:8000/ws/api/influxdb?op=WRITEPOINTS", String.format("data=%s", sb.toString()));
            // if report ok, clean themall
            for(Metric metric: metrics.values()){
                metric.reset();
            }
        } catch (IOException e) {
            e.printStackTrace();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            report();
        }
    }

    public static void main(String[] args) {
        MetricReporter metricReporter = new MetricReporter();
        metricReporter.addMetric("liuzhe_test", "qu1", 111L);
        metricReporter.addMetric("liuzhe_test", "qu2", 333L);
        metricReporter.addMetric("liuzhe_test", "qu1", 111L);
    }
}
