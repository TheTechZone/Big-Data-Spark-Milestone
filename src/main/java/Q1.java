import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

public class Q1 {

    /** MAKE SURE TO CHANGE THIS TO YOUR ACTUAL STARTING PATH**/
    private static final String STARTING_PATH = "/home/adrian/Documents/TUe/Quartile 3/2ID70 Data intensive systems/M2/tables/";
    /** Ok, thanks bye  **/

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
        Dataset<Row> dfDegrees = spark.read().schema(degreeSchema).option("header",true).csv(STARTING_PATH + "Degrees.table");

        StructType studentsSchema = new StructType(new StructField[]{
                //StudentId, StudentName, Address, BirthyearStudent, Gender
                new StructField("StudentId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("StudentName", DataTypes.StringType,true,Metadata.empty()),
                new StructField("Address", DataTypes.StringType, true, Metadata.empty()),
                new StructField("BirthyearStudent", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Gender", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dfStudents = spark.read().schema(studentsSchema).option("header", true).csv(STARTING_PATH +"Students.table");

        StructType studentRegSchema =  new StructType(new StructField[]{
                //StudentRegistrationId, StudentId, DegreeId, RegistrationYear
                new StructField("StudentRegistrationId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("StudentId", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField("DegreeId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("RegistrationYear", DataTypes.IntegerType, true, Metadata.empty()),
        });
        Dataset<Row> dfStudentRegistrations = spark.read().schema(studentRegSchema).option("header", true)
                .csv(STARTING_PATH +"StudentRegistrationsToDegrees.table");

        StructType teachersSchema =  new StructType(new StructField[]{
                //TeacherId, TeacherName, Address, BirthyearTeacher, Gender
                new StructField("TeacherId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("TeacherName", DataTypes.StringType,true,Metadata.empty()),
                new StructField("Address", DataTypes.StringType, true, Metadata.empty()),
                new StructField("BirthyearTeacher", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Gender", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> dfTeachers = spark.read().schema(teachersSchema).option("header", true).csv(STARTING_PATH +"Teachers.table");

        StructType coursesSchema =  new StructType(new StructField[]{
                //CourseId, CourseName, CourseDescription, DegreeId, ECTS
                new StructField("CourseId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("CourseName", DataTypes.StringType,true,Metadata.empty()),
                new StructField("CourseDescription", DataTypes.StringType, true, Metadata.empty()),
                new StructField("DegreeId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ECTS", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dfCourses = spark.read().schema(coursesSchema).option("header", true).csv(STARTING_PATH +"Courses.table");

        StructType courseOfferSchema =  new StructType(new StructField[]{
                //CourseOfferId, CourseId, Year, Quartile
                new StructField("CourseOfferId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("CourseId", DataTypes.StringType,true,Metadata.empty()),
                new StructField("Year", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Quartile", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dfCourseOffers = spark.read().schema(courseOfferSchema).option("header", true)
                .csv(STARTING_PATH +"CourseOffers.table");

        StructType courseRegSchema =  new StructType(new StructField[]{
                //CourseOfferId, StudentRegistrationId, Grade
                new StructField("CourseOfferId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("StudentRegistrationId", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField("Grade", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dfCourseRegistrations = spark.read().schema(courseRegSchema).option("header", true)
                .option("nullValue", "null")
                .csv(STARTING_PATH +"CourseRegistrations.table");

        try {
            dfCourseOffers.createTempView("courseOffers");
            dfCourseRegistrations.createTempView("courseRegistrations");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        Dataset<Row> q11 = spark.sql("SELECT AVG(cr.grade) FROM courseOffers as co, courseRegistrations as cr " +
                "WHERE co.Year = 2016 and co.quartile=2 AND cr.grade > 4 AND co.CourseOfferId = cr.CourseOfferId " +
                "GROUP BY co.CourseOfferId");
        q11.show();

        Dataset<Row> q12 = spark.sql("SELECT AVG(cr.grade) FROM courseRegistrations as cr " +
                "WHERE cr.grade > 4 AND cr.StudentRegistrationId = 3 ");
        q12.show();
    }
}
