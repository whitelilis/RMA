public class DailyTimer implements Runnable{
    MetricReporter metricReporter;
    public static final int reportInteval = 86400000;
    public DailyTimer(MetricReporter metricReporter){
        this.metricReporter = metricReporter;
    }

    @Override
    public void run() {
        while(true) {
            try {
                Thread.sleep(reportInteval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.metricReporter.report();
        }
    }
}
