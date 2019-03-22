import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import javax.xml.crypto.Data;
import java.awt.dnd.DropTarget;

public class Q1 {

    private static final String startingpath = "/home/adrian/Documents/TUe/Quartile 3/2ID70 Data intensive systems/M2/tables/";

    public static void main(String[] args){
        String master = "local[1]";
//        SparkConf conf = new SparkConf()
//                .setAppName(Q1.class.getName())
//                .setMaster(master);

        SparkSession spark = SparkSession
                .builder()
                .appName(Q1.class.getName())
                .config("spark.master", "local")
                .getOrCreate();


        StructType degreeSchema = new StructType(new StructField[]{
            new StructField("DegreeID", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("Dept", DataTypes.StringType,true,Metadata.empty()),
            new StructField("Description", DataTypes.StringType, true, Metadata.empty()),
            new StructField("TotalECTS", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dfDegrees = spark.read().schema(degreeSchema).option("header",true).csv(startingpath + "Degrees.table");

        StructType studentsSchema = new StructType(new StructField[]{
                //StudentId, StudentName, Address, BirthyearStudent, Gender
                new StructField("StudentId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("StudentName", DataTypes.StringType,true,Metadata.empty()),
                new StructField("Address", DataTypes.StringType, true, Metadata.empty()),
                new StructField("BirthyearStudent", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Gender", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dfStudents = spark.read().schema(studentsSchema).option("header", true).csv(startingpath+"Students.table");

        StructType studentRegSchema =  new StructType(new StructField[]{
                //StudentRegistrationId, StudentId, DegreeId, RegistrationYear
                new StructField("StudentRegistrationId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("StudentId", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField("DegreeId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("RegistrationYear", DataTypes.IntegerType, true, Metadata.empty()),
        });
        Dataset<Row> dfStudentRegistrations = spark.read().schema(studentRegSchema).option("header", true)
                .csv(startingpath+"StudentRegistrationsToDegrees.table");

        StructType teachersSchema =  new StructType(new StructField[]{
                //TeacherId, TeacherName, Address, BirthyearTeacher, Gender
                new StructField("TeacherId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("TeacherName", DataTypes.StringType,true,Metadata.empty()),
                new StructField("Address", DataTypes.StringType, true, Metadata.empty()),
                new StructField("BirthyearTeacher", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Gender", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> dfTeachers = spark.read().schema(teachersSchema).option("header", true).csv(startingpath+"Teachers.table");

        StructType coursesSchema =  new StructType(new StructField[]{
                //CourseId, CourseName, CourseDescription, DegreeId, ECTS
                new StructField("CourseId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("CourseName", DataTypes.StringType,true,Metadata.empty()),
                new StructField("CourseDescription", DataTypes.StringType, true, Metadata.empty()),
                new StructField("DegreeId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ECTS", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dfCourses = spark.read().schema(coursesSchema).option("header", true).csv(startingpath+"Courses.table");

        StructType courseOfferSchema =  new StructType(new StructField[]{
                //CourseOfferId, CourseId, Year, Quartile
                new StructField("CourseOfferId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("CourseId", DataTypes.StringType,true,Metadata.empty()),
                new StructField("Year", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Quartile", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dfCourseOffers = spark.read().schema(courseOfferSchema).option("header", true)
                .csv(startingpath+"CourseOffers.table");

        StructType courseRegSchema =  new StructType(new StructField[]{
                //CourseOfferId, StudentRegistrationId, Grade
                new StructField("CourseOfferId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("StudentRegistrationId", DataTypes.StringType,true,Metadata.empty()),
                new StructField("Grade", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dfCourseRegistrations = spark.read().schema(coursesSchema).option("header", true)
                .csv(startingpath+"CourseRegistrations.table");

        dfCourseOffers.sort(dfCourseOffers.col("CourseId")).show();
    }
}
