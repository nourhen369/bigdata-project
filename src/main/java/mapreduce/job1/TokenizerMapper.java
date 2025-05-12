package mapreduce.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text sector = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header line
        if (key.get() == 0 && value.toString().contains("Building ID")) {
            return;
        }

        // Robust split for CSV
        String[] fields = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        if (fields.length > 3) {
            String sectorValue = fields[3].trim();  // "Cohort - Sector"
            if (!sectorValue.isEmpty()) {
                sector.set(sectorValue);
                context.write(sector, one);
            }
        }
    }
}
