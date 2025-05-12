package job2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperMeanSize extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Ignore header
        if (line.startsWith("Building ID")) return;

        String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1); // handle quoted fields

        if (fields.length < 8) return;

        String communityAreaName = fields[6].trim();
        String cohortSize = fields[4].trim().replaceAll("\"", "");

        int size = parseSize(cohortSize);
        if (size > 0) {
            context.write(new Text(communityAreaName), new IntWritable(size));
        }
    }

    private int parseSize(String sizeStr) {
        if (sizeStr.contains("50,000")) {
            return 50000;
        } else if (sizeStr.contains("250,000")) {
            return 250000;
        } else if (sizeStr.contains("200,000")) {
            return 200000;
        } else if (sizeStr.contains("100,000")) {
            return 100000;
        }
        return -1; // invalid
    }
}

