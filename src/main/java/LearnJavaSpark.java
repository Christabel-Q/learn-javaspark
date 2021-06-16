import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.File;
import java.io.InputStream;
import java.util.function.Consumer;

public class LearnJavaSpark {
    private static SparkSession sparkSession = null;
    private static JavaSparkContext sparkContext = null;

    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir", "C:\\Hadoop");
        System.out.println("Let's get started!");

        sparkSession = SparkSession.builder()
                .master("local")
                .appName("LearnJavaSpark")
                .getOrCreate();
        sparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
//        sparkContext.setLogLevel("Error");
        System.out.print(sparkSession.toString());

        // perform on local machine
//        System.out.println("Performing read CSV and write Parquet on local machine");
//        String localSource = "C:\\Users\\ChristabelTeo\\OneDrive - Quadrant Global Pte Ltd\\Documents\\Misc\\QD Figures\\2019\\csv\\";
//        String localDest = "C:\\Users\\ChristabelTeo\\OneDrive - Quadrant Global Pte Ltd\\Documents\\Misc\\QD Figures\\2019\\parquet\\";
//        readCsvWriteParquet(localSource, localDest);

        // perform on emr
        System.out.println("Performing read CSV and write Parquet on EMR");
        String SOURCE_POC_VENPATH = "s3://quadrant-poc/sync-test/venpath-source/";
        String DESTINATION_POC_VENPATH = "s3://quadrant-poc/sync-test/venpath-dest/";
        s3ReadCsvWriteParquet(SOURCE_POC_VENPATH, DESTINATION_POC_VENPATH);

        // end
        sparkContext.close();
        sparkSession.stop();
        System.out.println("And.. we are done!");
    }

    public static void readCsvWriteParquet(String sourceDir, String destDir) {
        File sourceFolder = new File(sourceDir);
        File[] fileList = sourceFolder.listFiles();

        try {
            for (final File fileEntry : sourceFolder.listFiles()) {
                String filename = fileEntry.getName();
                System.out.println("Reading: " + filename);

                // reading csv files
                Dataset<Row> file = sparkSession.read()
                        .format("csv")
                        .option("header","true")
                        .load(sourceDir + filename);
                file.persist();
                file.show(5);

                // writing files into parquet
                final String parquet = "parquet";
                String destFilename = destDir + filename.substring(0, filename.length()-3) + parquet;
                System.out.println("Writing to: " + destFilename);
                file.write().format(parquet).save(destFilename);
                System.out.println("Done\n");

            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    public static void s3ReadCsvWriteParquet(String sourceDir, String destDir) {

//        S3Client s3Client = S3Client.builder().build();
//        ListObjectsRequest req = new ListObjectsRequest()
//                .withBucketName("quadrant-poc")
//                .withPrefix("sync-test/venpath-source/");
//
//        ListObjectsV2Iterable responses = s3Client.listObjectsV2Paginator((
//                Consumer<software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder>) req);
//
//        for (ListObjectsV2Response page : responses) {
//            page.contents().forEach(object -> {
//                String name = object.key();
//                System.out.println("Reading: " + name);
//
//            });
//        }

        // read files from s3 bucket path
        System.out.println("Reading files from " + sourceDir);
        Dataset<Row> file = sparkSession
                .read()
                .format("CSV")
                .option("header", "false")
                .option("delimiter", ",")
                .load(sourceDir);
        file.printSchema();
        file.show(5);

        // write files into another
        System.out.println("Writing files into " + sourceDir);
        int coalesceLimit = 20;
        file.coalesce(coalesceLimit)
                .write()
                .partitionBy("t_date", "country")
                .parquet(destDir);

    }
}
