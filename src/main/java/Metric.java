public class Metric {
    public String measurement;
    public static final String partTemplate = "{\"measurement\":\"%s\",\"tags\":{\"queue\":\"%s\"},\"fields\":{\"mt\":%d}}";
    public String queueName;
    public long mt;

    public Metric(String measurement, String queueName, long mt){
        this.measurement = measurement;
        this.queueName = queueName;
        this.mt = mt;
    }

    public String getString(){
        synchronized (this) {
            return String.format(partTemplate, measurement, queueName, mt);
        }
    }

    public void reset(){
        synchronized (this){
            this.mt = 0;
        }
    }

    public void addMt(long item){
        synchronized (this){
            this.mt = this.mt + item;
        }
    }
}
