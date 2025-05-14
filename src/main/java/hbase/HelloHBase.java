package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class HelloHBase {
    public static void main(String[] args) {
        // Load CSV from resources
        InputStream is = HelloHBase.class.getClassLoader().getResourceAsStream("/input/file.csv");
        if (is == null) {
            System.err.println("Fichier introuvable dans les resources !");
            return;
        }

        // Create HBase configuration
        Configuration config = HBaseConfiguration.create();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(is));
             Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("energy"))) {

            String line;
            boolean skipHeader = true;

            while ((line = br.readLine()) != null) {
                if (skipHeader) {
                    skipHeader = false; // skip CSV header
                    continue;
                }

                String[] fields = line.split(",", -1); // allow empty fields
                if (fields.length < 12) {
                    System.out.println("Ligne invalide: " + line);
                    continue;
                }

                String rowKey = fields[0]; // Building ID
                Put put = new Put(Bytes.toBytes(rowKey));

                // loc
                put.addColumn(Bytes.toBytes("loc"), Bytes.toBytes("address"), Bytes.toBytes(fields[1]));
                put.addColumn(Bytes.toBytes("loc"), Bytes.toBytes("zip"), Bytes.toBytes(fields[2]));
                put.addColumn(Bytes.toBytes("loc"), Bytes.toBytes("comm_name"), Bytes.toBytes(fields[6]));
                put.addColumn(Bytes.toBytes("loc"), Bytes.toBytes("comm_num"), Bytes.toBytes(fields[7]));
                put.addColumn(Bytes.toBytes("loc"), Bytes.toBytes("ward"), Bytes.toBytes(fields[8]));

                // usage
                put.addColumn(Bytes.toBytes("usage"), Bytes.toBytes("sector"), Bytes.toBytes(fields[3]));
                put.addColumn(Bytes.toBytes("usage"), Bytes.toBytes("size"), Bytes.toBytes(fields[4]));

                // meta
                put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("ver_year"), Bytes.toBytes(fields[5]));

                // geo
                put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("lat"), Bytes.toBytes(fields[9]));
                put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("lon"), Bytes.toBytes(fields[10]));
                put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("location"), Bytes.toBytes(fields[11]));

                // Insert into HBase
                table.put(put);
            }

            System.out.println("Importation terminée avec succès dans la table HBase 'energy'.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
