import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.data.general.DatasetUtilities;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.general.PieDataset;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by liuzhe on 16/2/4.
 */
public class PiePng {

    public static void makeJPG(String outPath, String title, HashMap<String, Long> data) throws IOException {
        String forWritePostFix = ".bak";
        String forDelPostfix     = ".del";

        DefaultPieDataset dataset = new DefaultPieDataset();
        for(Map.Entry<String, Long> i : data.entrySet()){
            dataset.setValue(i.getKey(), i.getValue());
        }


        PieDataset newSet  = DatasetUtilities.createConsolidatedPieDataset(dataset, "other", 0.01); // bug, not percent;

        JFreeChart chart = ChartFactory.createPieChart(
                title, // chart title
                newSet, // data
                true, // include legend
                true,
                true);

        PiePlot pieplot = (PiePlot) chart.getPlot(); //通过JFreeChart 对象获得
        pieplot.setNoDataMessage("无数据可供显示！"); // 没有数据的时候显示的内容
        pieplot.setLabelGenerator(new StandardPieSectionLabelGenerator(
                ("{0}: ({2})"), NumberFormat.getNumberInstance(),
                new DecimalFormat("0.00%")));

        int width = 640; /* Width of the image */
        int height = 480; /* Height of the image */

        String forDelete = outPath + forDelPostfix;
        String forWrite = outPath + forWritePostFix;
        ChartUtilities.saveChartAsJPEG(new File(forWrite), chart, width, height);

        if(new File(outPath).exists()){
            new File(outPath).renameTo(new File(forDelete));
            new File(forWrite).renameTo(new File(outPath));
            new File(forDelete).delete();
        }else{
            new File(forWrite).renameTo(new File(outPath));
        }
    }
    public static void main(String[] args) throws Exception {
        HashMap<String, Long> h = new HashMap<>();
        HashMap<String, Long> h2 = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(new File("/tmp/mapping_result")));
        String line = br.readLine();
        while(line != null){
            String[] parts = line.split(" ");
            String key = new String(parts[0]);
            String[] keyParts = key.split(";", -1);
            key = keyParts[keyParts.length - 2];
            h.put(parts[0], Long.parseLong(parts[1]));
            if(h2.containsKey(key)) {
                h2.put(key, Long.parseLong(parts[1]) + h2.get(key));
            }else{
                h2.put(key, Long.parseLong(parts[1]));
            }
            line = br.readLine();
        }
        makeJPG("/tmp/test_sim.jpg", "test", h2);
        makeJPG("/tmp/test.jpg", "test", h);
    }
}
