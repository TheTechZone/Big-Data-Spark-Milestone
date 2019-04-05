import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;


public class Q2 {
    /**
     * Must be changed the starting path to the actual location
     **/
    private static final String STARTING_PATH = "/home/adrian/Documents/TUe/Quartile 3/2ID70 Data intensive systems/M2/tables/";

    public static void main(String[] args) {
        // Spark initializations
        String master = "local[2]";

        SparkConf conf = new SparkConf()
                .setAppName(Q2.class.getName())
                .setMaster(master);

        SparkSession spark = SparkSession
                .builder()
                .appName(Q2.class.getName())
                .config("spark.master", master)
                .getOrCreate();

        //  1. Define the schema for our data

        StructType courseRegSchema = new StructType(new StructField[]{
                //CourseOfferId, StudentRegistrationId, Grade
                new StructField("CourseOfferId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("StudentRegistrationId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Grade", DataTypes.IntegerType, true, Metadata.empty())
        });

        StructType courseOfferSchema = new StructType(new StructField[]{
                //CourseOfferId, CourseId, Year, Quartile
                new StructField("CourseOfferId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("CourseId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Year", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Quartile", DataTypes.IntegerType, true, Metadata.empty())
        });

        // Create RDD's (we use the spark.read() for convenience and convert to RDDs)

        JavaRDD<Row> rddCourseReg = spark.read().schema(courseRegSchema).option("header", true)
                .option("nullValue", "null")
                .csv(STARTING_PATH + "CourseRegistrations.table").javaRDD();

        JavaRDD<Row> rddCourseOffers = spark.read().schema(courseOfferSchema).option("header", true)
                .csv(STARTING_PATH + "CourseOffers.table").javaRDD();


        /**
         * SELECT AVG(GRADE)
         * FROM sr, co
         * WHERE sr.cID = co.coID and sr.grade > 5 and co.cID = %1% and co.quartile = %2%
         *  and co.year = %3%
         */

        int param1 = 36413;     /* course id */
        int param2 = 3;         /* quartile */
        int param3 = 2010;      /* year */

        // Defining the params specified in the query

        JavaRDD<ArrayList> mappedCReg = rddCourseReg.map((Row a) -> {
            ArrayList<Integer> l = new ArrayList<>();
            scala.collection.Iterator it = a.toSeq().toList().iterator();
            while (it.hasNext()) {
                l.add((Integer) it.next());
            }
            l.add(1);  // augment tuple with origin
            return l;
        });

        JavaRDD<ArrayList> mappedCOffer = rddCourseOffers.map((Row a) -> {
            ArrayList<Integer> l = new ArrayList<>();
            scala.collection.Iterator it = a.toSeq().toList().iterator();
            while (it.hasNext()) {
                l.add((Integer) it.next());
            }
            l.add(2); // augment tuple with origin
            return l;
        });

        // Turning the RDDs into pairRdds with courseOfferingId as key
        JavaPairRDD<Integer, ArrayList<Integer>> cReg = mappedCReg.mapToPair(a -> {
            return new Tuple2<Integer, ArrayList<Integer>>((Integer) a.get(0), a);
        });

        JavaPairRDD<Integer, ArrayList<Integer>> cOffer = mappedCOffer.mapToPair(a -> {
            return new Tuple2<Integer, ArrayList<Integer>>((Integer) a.get(0), a);
        });

        // Construct the rdd of their union
        JavaRDD<ArrayList> resultRdd = cReg.union(cOffer)
                .groupByKey()
                .flatMap(a -> {
                    ArrayList<ArrayList<Integer>> list1 = new ArrayList<>();
                    ArrayList<ArrayList<Integer>> list2 = new ArrayList<>();

                    a._2().forEach(item -> {
                        if (item.get(item.size() - 1) == 1) {
                            list1.add(item);
                        } else {
                            list2.add(item);
                        }
                    });

                    ArrayList<ArrayList> rez = new ArrayList<>();
                    for (int i = 0; i < list1.size(); i++) {
                        for (int j = 0; j < list2.size(); j++) {
                            ArrayList temp = new ArrayList();
                            for (int k = 0; k < list1.get(i).size(); k++) {
                                temp.add(list1.get(i).get(k));
                            }
                            for (int k = 0; k < list2.get(j).size(); k++) {
                                temp.add(list2.get(j).get(k));
                            }
                            rez.add(temp);
                        }
                    }

                    Iterator<ArrayList> itter = rez.iterator();
                    return itter;
                });


        // Filtering the join space (cartesian product) of the RDD in order to answer
        // the given query
        JavaRDD<ArrayList> filterRdd = resultRdd.filter(a -> (
                (Integer) a.get(5) == param1 && (Integer) a.get(7) == param2 && (Integer) a.get(6) == param3
        )).filter(a -> a.get(2) != null && ((Integer) a.get(2) > 4));

        // Computing the grade locally. We reduce the sume of the grades over 5
        int gradeSum = filterRdd.map(a -> (Integer) a.get(2)).reduce((a, b) -> (a + b));
        // and the sum
        int count = (int) filterRdd.count();
        System.out.println((double) gradeSum / count); // Debug print
    }

}
