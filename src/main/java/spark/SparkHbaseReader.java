package spark;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkHbaseReader {

    public static void main(String[] args) throws IOException {

        // Configuration Spark
        SparkConf sparkConf = new SparkConf().setAppName("HBaseCommunityCounter").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // Configuration HBase
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "localhost");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "energy");

        // Lire la table HBase dans un RDD
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);

        // Extraire la colonne "loc:comm_num" (nom de la zone)
        JavaRDD<String> communityAreas = hBaseRDD.map(tuple -> {
            Result result = tuple._2;
            byte[] valueBytes = result.getValue(Bytes.toBytes("loc"), Bytes.toBytes("comm_num"));
            return valueBytes != null ? Bytes.toString(valueBytes) : "UNKNOWN";
        });

        // Compter les occurrences par zone
        JavaPairRDD<String, Integer> countsByArea = communityAreas
                .mapToPair(name -> new Tuple2<>(name, 1))
                .reduceByKey(Integer::sum);

        // Afficher les résultats
        List<Tuple2<String, Integer>> output = countsByArea.collect();
        for (Tuple2<String, Integer> entry : output) {
            System.out.println("Zone: " + entry._1 + " | Bâtiments: " + entry._2);
        }

        jsc.close();
    }
}
