import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;

import org.spark_project.jetty.client.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.spark.sql.Encoders.*;


public class SparkTest {

    public static void main(String[] args) {
        String master = "local[1]";
        SparkConf conf = new SparkConf()
                .setAppName(ExampleWithMapReduce.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        Random rn = new Random(234);

        ArrayList<String> persons = new ArrayList<>();
        persons.add("John,10,M,1111");
        persons.add("Jill,15,F,2222");
        persons.add("James,20,M,3333");
        for (int i=0;i<1000;i++) persons.add("person"+i+"," + rn.nextInt(100) +"," + (rn.nextBoolean()?"M":"F") + "," + i);

        JavaRDD<String> rddPersons = sc.parallelize(persons);

        rddPersons.cache(); // pin in ram

        // make an rdd with all <id, names>
        JavaPairRDD<Integer, String> onlyIdsAndNames = rddPersons.mapToPair(p -> {
            String[] vals = p.split(",") ;
            int id = Integer.parseInt(vals[3]);
            Tuple2<Integer,String> r = new Tuple2<>(id, vals[0]);
            return r;
        });

        Map<Integer, String> idsAndNames = onlyIdsAndNames.collectAsMap();
        for (Map.Entry<Integer,String> e: idsAndNames.entrySet()) {
            System.out.println(e.getKey() + " has an id of " + e.getValue());
        }

        onlyIdsAndNames.collectAsMap().forEach((k,v) -> {
            System.out.println(v + " has an id of " + k);
        });

        // now make datasets/data frames
        StructType schema_persons = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("name", DataTypes.StringType, false),
                        DataTypes.createStructField("age", DataTypes.IntegerType, false),
                        DataTypes.createStructField("gender", DataTypes.StringType, false),
                        DataTypes.createStructField("id", DataTypes.IntegerType, false)});

        StructType schema_addresses = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("id", DataTypes.IntegerType, false),
                        DataTypes.createStructField("address", DataTypes.StringType, false)});

        // now we have RDD of strings, need to have RDD of rows

        JavaRDD<Row> rddPersonsRows = rddPersons.map( p -> {
            String[] breakToCols = p.split(",");
            Object[] attributes =  new Object[4];
            attributes[0]=breakToCols[0];
            attributes[1]=Integer.parseInt(breakToCols[1]);
            attributes[2]=breakToCols[2];
            attributes[3]=Integer.parseInt(breakToCols[3]);
            return RowFactory.create(attributes);
        } );  // where is this processing performed?

        SparkSession sparkSQL = SparkSession.builder().appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value").getOrCreate();

        Dataset<Row> personsDataset = sparkSQL.sqlContext().createDataFrame(rddPersonsRows, schema_persons);

        // I can chain the operations on the dataset
        personsDataset.filter("age<10").select("name", "age").foreach(p-> System.out.println(p.getString(0) + " is " + p.getInt(1) + " years old."));

        // I can also make it a table
        personsDataset.registerTempTable("personsTable");
        Dataset<Row> sqlResult = sparkSQL.sqlContext().sql("SELECT name,age FROM personsTable");
        sqlResult.foreach(p-> System.out.println(p.getString(0) + " is " + p.getInt(1) + " years old."));

        // or I can break it to generate intermediary datasets
        Dataset<Row> filterResults = personsDataset.filter("age<10");
        Dataset<Row> selectResults = filterResults.select("name", "age");
        selectResults.foreach(p-> System.out.println(p.getString(0) + " is " + p.getInt(1) + " years old."));

        // due to lazy evaluation, in both cases execution will start when
        // invoking an action, e.g., foreach or collect


        // now let's execute a join

        // need a second 'table', but now let's construct it with structure from the start
        ArrayList<Address> addresses = new ArrayList<>();
        addresses.add(new Address(1111, "his house"));
        addresses.add(new Address(2222, "her house"));
        addresses.add(new Address(3333, "he stays with Jill"));
        for (int i=0;i<1000;i++) addresses.add(new Address(i,"address "+i));
        JavaRDD<Address> rddAddresses = sc.parallelize(addresses);

        // Dataset construction
        Dataset<Address> addressesDataset = sparkSQL.sqlContext().createDataset(JavaRDD.toRDD(rddAddresses),
                Encoders.bean(Address.class));


        Dataset<Row> joinResult = personsDataset.join(addressesDataset, "id");
        // and now print only name and address
        // can do a foreach as before, but now let's use collectAsList which brings everything at the driver machine
        List<Row> results = joinResult.select("name", "address").collectAsList();
        for (Row r:results) {
            // instead of Row's getInt and getString, I can also get the attribute values using the attribute names
            System.out.println("JoinResult [" + r.getAs("name") + "," + r.getAs("address") + "]");
        }
    }

}
