import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

class Random implements Serializable {
    /**
     * XORShift algorithm since Java.util.Random is terrible
     */
    private static volatile long state = 0xCAFEBABE; // initial non-zero value

    public static final long nextLong() {
        long a = state;
        state = xorShift64(a);
        return a;
    }

    public static final long xorShift64(long a) {
        a ^= (a << 21);
        a ^= (a >>> 35);
        a ^= (a << 4);
        return a;
    }

    public static final int random(int n) {
        if (n < 0) throw new IllegalArgumentException();
        long result = ((nextLong() >>> 32) * n) >> 32;
        return (int) result;
    }

    public static final int nextInt(int n) {
        return random(n);
    }
}

public class Q3 {
    // Class has not be finished, comments used to explain
    // the structure of the code

    /**
     * Must be changed the starting path to the actual location
     **/
    static final String STARTING_PATH = "/home/adrian/Documents/TUe/Quartile 3/2ID70 Data intensive systems/M2/tables/";
    static final String master = "local[4]";

    public static void main(String[] args) {
        // Spark initializations
        SparkConf conf = new SparkConf().setAppName(Q3.class.getName()).setMaster(master);
        SparkContext sc = SparkContext.getOrCreate(conf);
        SparkSession spark = SparkSession.builder().getOrCreate();

        // Loading the data (leveraging the spark dataset api)
        StructType courseRegSchema = new StructType(new StructField[]{
                //CourseOfferId, StudentRegistrationId, Grade
                new StructField("CourseOfferId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("StudentRegistrationId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Grade", DataTypes.IntegerType, true, Metadata.empty())
        });

        JavaRDD rddCourseReg = spark.read().schema(courseRegSchema).option("header", true)
                .option("nullValue", "null")
                .csv(STARTING_PATH + "CourseRegistrations.table").javaRDD();
        JavaRDD rddQuantilePoints = spark.read()
                .textFile(STARTING_PATH + "QuantilePoints.table.txt")
                .javaRDD()
                .filter(a -> (!a.equals("QuantilePoints"))).map(a -> Integer.parseInt(a));

        double r = sc.defaultParallelism() * 1.0; // determine the default spark parallelism

        // calling the genering theta join algorithm
        new Q3().q3a(rddCourseReg, rddQuantilePoints, r);
    }

    public void q3a(JavaRDD<Row> S, JavaRDD<Row> T, double r) {
        // Computing the cardinality statistics
        final long cardinalS = S.count();
        final long cardinalT = T.count();
        int ct, cs;
        double joinWidth, joinHeight;
        // optimal ration. as described in the paper
        double optimalRatio = Math.sqrt((cardinalT * cardinalS) / r);

        // determine the best strategy to use
        if (cardinalS % optimalRatio == 0 && cardinalT % optimalRatio == 0) {
            joinWidth = optimalRatio;
            joinHeight = optimalRatio;

            ct = (int) (cardinalT / optimalRatio);
            cs = (int) (cardinalS / optimalRatio);
        } else if (cardinalS < cardinalT / r) {
            joinHeight = cardinalS;
            joinWidth = cardinalT / r;
            ct = (int) r;
            cs = 1;
        } else {
            cs = Math.max((int) (cardinalS / optimalRatio), 1);
            ct = Math.max((int) (cardinalT / optimalRatio), 1); // taking 1 as min to
            // to handle edge cases

            joinHeight = (1 + 1 / (Math.min(cs, ct))) * optimalRatio;
            joinWidth = (1 + 1 / (Math.min(cs, ct))) * optimalRatio;
        }

        // Augumenting the data
        JavaPairRDD pairSRdd =
                S.mapToPair(s -> new Tuple2<>(1, s)); // 1 for rows
        JavaPairRDD<Integer, Row> pairTRdd =
                T.mapToPair(t -> new Tuple2<>(2, t));// 2 for cols

        // Union the rdds and apply the randomized algorithm
        JavaPairRDD unionRdd = pairSRdd.union(pairTRdd);
        unionRdd.flatMap(a -> {
            Tuple2 b = (Tuple2) a;
            ArrayList regions = new ArrayList();

            // mapping each tuple from to on S to the same region R
            // creating pairs <R,T> with R denotes an Row/Col region
            // in the join space
            if ((Integer) b._1() == 1) {
                // row tuple
                int row = Random.nextInt((int) cardinalS);
                int startPoint = (int) (row / joinHeight + 1);
                int endingPoint = startPoint + ct;
                for (int i = startPoint; i < endingPoint; i++) {
//                  regions.add(i);
//                    yield new Tuple2<>(i, a);

                }
            } else {
                // column tuple
                int col = Random.nextInt((int) cardinalT);
                int startPoint = (int) (col / joinWidth + 1);
                int endingPoint = ct * cs + 1;
                for (int i = startPoint; i < endingPoint; i += ct) {
                    regions.add(i);
                }
            }
            ArrayList results = new ArrayList();
            for (int i=0; i<regions.size();i++) {
                results.add(new Tuple2<>(regions.get(i), a));
            }
            return results.iterator();
        });
        // group them by the region key
        JavaPairRDD grouped = unionRdd.groupByKey();

        // reduce them by splitting the S and T tuples

        // join the s and T tuples using a join condition

        // output the result
    }

    public void q3b() {
        // implies using q3a() and modifying the join condition.
    }
}
