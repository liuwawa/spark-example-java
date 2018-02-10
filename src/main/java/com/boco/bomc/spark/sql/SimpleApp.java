package com.boco.bomc.spark.sql;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import static org.apache.spark.sql.functions.col;

import scala.Tuple2;

public class SimpleApp {
	
	private static SparkConf conf = new SparkConf().setAppName(SimpleApp.class.getName());
	private static JavaSparkContext sc = new JavaSparkContext(conf);
	
	static class GetLength implements Function<String, Integer> {
		public Integer call(String s) {
			return s.length();
		}
	}

	static class Sum implements Function2<Integer, Integer, Integer> {
		public Integer call(Integer a, Integer b) {
			return a + b;
		}
	}
	
	static class Person implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private String name;
		private int age;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
	}
	
	static class MyAverage extends UserDefinedAggregateFunction {

		private static final long serialVersionUID = 1L;
		
		private StructType inputSchema;
		private StructType bufferSchema;

		public MyAverage() {
			List<StructField> inputFields = new ArrayList<>();
			inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
			inputSchema = DataTypes.createStructType(inputFields);

			List<StructField> bufferFields = new ArrayList<>();
			bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
			bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
			bufferSchema = DataTypes.createStructType(bufferFields);
		}

		// Data types of input arguments of this aggregate function
		@Override
		public StructType inputSchema() {
			return inputSchema;
		}

		// Data types of values in the aggregation buffer
		@Override
		public StructType bufferSchema() {
			return bufferSchema;
		}

		// The data type of the returned value
		@Override
		public DataType dataType() {
			return DataTypes.DoubleType;
		}

		// Whether this function always returns the same output on the identical
		// input
		@Override
		public boolean deterministic() {
			return true;
		}

		// Initializes the given aggregation buffer. The buffer itself is a
		// `Row` that in addition to
		// standard methods like retrieving a value at an index (e.g., get(),
		// getBoolean()), provides
		// the opportunity to update its values. Note that arrays and maps
		// inside the buffer are still
		// immutable.
		@Override
		public void initialize(MutableAggregationBuffer buffer) {
			buffer.update(0, 0L);
			buffer.update(1, 0L);
		}

		// Updates the given aggregation buffer `buffer` with new input data
		// from `input`
		@Override
		public void update(MutableAggregationBuffer buffer, Row input) {
			if (!input.isNullAt(0)) {
				long updatedSum = buffer.getLong(0) + input.getLong(0);
				long updatedCount = buffer.getLong(1) + 1;
				buffer.update(0, updatedSum);
				buffer.update(1, updatedCount);
			}
		}

		// Merges two aggregation buffers and stores the updated buffer values
		// back to `buffer1`
		@Override
		public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
			long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
			long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
			buffer1.update(0, mergedSum);
			buffer1.update(1, mergedCount);
		}

		// Calculates the final result
		public Double evaluate(Row buffer) {
			return ((double) buffer.getLong(0)) / buffer.getLong(1);
		}
	}
	
	static class Employee implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private String name;
		private long salary;
		
		public Employee() {
			super();
		}
		
		public Employee(String name, long salary) {
			super();
			this.name = name;
			this.salary = salary;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public long getSalary() {
			return salary;
		}
		public void setSalary(long salary) {
			this.salary = salary;
		}

	}

	static class Average implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private long sum;
		private long count;
		
		public Average() {
			super();
		}
		public Average(long sum, long count) {
			super();
			this.sum = sum;
			this.count = count;
		}
		public long getSum() {
			return sum;
		}
		public void setSum(long sum) {
			this.sum = sum;
		}
		public long getCount() {
			return count;
		}
		public void setCount(long count) {
			this.count = count;
		}

	}

	static class My_Average extends Aggregator<Employee, Average, Double> {
		private static final long serialVersionUID = 1L;

		// A zero value for this aggregation. Should satisfy the property that
		// any b + zero = b
		@Override
		public Average zero() {
			return new Average(0L, 0L);
		}

		// Combine two values to produce a new value. For performance, the
		// function may modify `buffer`
		// and return it instead of constructing a new object
		@Override
		public Average reduce(Average buffer, Employee employee) {
			long newSum = buffer.getSum() + employee.getSalary();
			long newCount = buffer.getCount() + 1;
			buffer.setSum(newSum);
			buffer.setCount(newCount);
			return buffer;
		}

		// Merge two intermediate values
		@Override
		public Average merge(Average b1, Average b2) {
			long mergedSum = b1.getSum() + b2.getSum();
			long mergedCount = b1.getCount() + b2.getCount();
			b1.setSum(mergedSum);
			b1.setCount(mergedCount);
			return b1;
		}

		// Transform the output of the reduction
		@Override
		public Double finish(Average reduction) {
			return ((double) reduction.getSum()) / reduction.getCount();
		}

		// Specifies the Encoder for the intermediate value type
		@Override
		public Encoder<Average> bufferEncoder() {
			return Encoders.bean(Average.class);
		}

		// Specifies the Encoder for the final output value type
		@Override
		public Encoder<Double> outputEncoder() {
			return Encoders.DOUBLE();
		}
	}

	private static void applyDataset1(){
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		// Create an instance of a Bean class
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);

		// Encoders are created for Java beans
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(
		  Collections.singletonList(person),
		  personEncoder
		);
		javaBeanDS.show();
		// +---+----+
		// |age|name|
		// +---+----+
		// | 32|Andy|
		// +---+----+

		// Encoders for most common types are provided in class Encoders
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map(
		    (MapFunction<Integer, Integer>) value -> value + 1,
		    integerEncoder);
		transformedDS.collect(); // Returns [2, 3, 4]

		// DataFrames can be converted to a Dataset by providing a class. Mapping based on name
		String path = "examples/src/main/resources/people.json";
		Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
		peopleDS.show();
		// +----+-------+
		// | age|   name|
		// +----+-------+
		// |null|Michael|
		// |  30|   Andy|
		// |  19| Justin|
		// +----+-------+
		
	}
	
	private static void applyRDDsToDataset(){
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		// Create an RDD of Person objects from a text file
		JavaRDD<Person> peopleRDD = spark.read()
		  .textFile("examples/src/main/resources/people.txt")
		  .javaRDD()
		  .map(line -> {
		    String[] parts = line.split(",");
		    Person person = new Person();
		    person.setName(parts[0]);
		    person.setAge(Integer.parseInt(parts[1].trim()));
		    return person;
		  });

		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
		// Register the DataFrame as a temporary view
		peopleDF.createOrReplaceTempView("people");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

		// The columns of a row in the result can be accessed by field index
		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
		    (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
		    stringEncoder);
		teenagerNamesByIndexDF.show();
		// +------------+
		// |       value|
		// +------------+
		// |Name: Justin|
		// +------------+

		// or by field name
		Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
		    (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
		    stringEncoder);
		teenagerNamesByFieldDF.show();
		// +------------+
		// |       value|
		// +------------+
		// |Name: Justin|
		// +------------+
	}
	
	private static void applySchema(){
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		// Create an RDD
		JavaRDD<String> peopleRDD = spark.sparkContext()
		  .textFile("examples/src/main/resources/people.txt", 1)
		  .toJavaRDD();

		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows
		JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
		  String[] attributes = record.split(",");
		  return RowFactory.create(attributes[0], attributes[1].trim());
		});

		// Apply the schema to the RDD
		Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

		// Creates a temporary view using the DataFrame
		peopleDataFrame.createOrReplaceTempView("people");

		// SQL can be run over a temporary view created using DataFrames
		Dataset<Row> results = spark.sql("SELECT name FROM people");

		// The results of SQL queries are DataFrames and support all the normal RDD operations
		// The columns of a row in the result can be accessed by field index or by field name
		Dataset<String> namesDS = results.map(
		    (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
		    Encoders.STRING());
		namesDS.show();
		// +-------------+
		// |        value|
		// +-------------+
		// |Name: Michael|
		// |   Name: Andy|
		// | Name: Justin|
		// +-------------+
	}
	
	private static void userDefinedAggregateFunc(){
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		// Register the function to access it
		spark.udf().register("myAverage", new MyAverage());

		Dataset<Row> df = spark.read().json("examples/src/main/resources/employees.json");
		df.createOrReplaceTempView("employees");
		df.show();
		// +-------+------+
		// |   name|salary|
		// +-------+------+
		// |Michael|  3000|
		// |   Andy|  4500|
		// | Justin|  3500|
		// |  Berta|  4000|
		// +-------+------+

		Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
		result.show();
		// +--------------+
		// |average_salary|
		// +--------------+
		// |        3750.0|
		// +--------------+
		
	}
	
	private static void applyUserDefinedAggregateFunc2(){
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
		String path = "examples/src/main/resources/employees.json";
		Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
		ds.show();
		// +-------+------+
		// |   name|salary|
		// +-------+------+
		// |Michael|  3000|
		// |   Andy|  4500|
		// | Justin|  3500|
		// |  Berta|  4000|
		// +-------+------+

		My_Average myAverage = new My_Average();
		// Convert the function to a `TypedColumn` and give it a name
		TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
		Dataset<Double> result = ds.select(averageSalary);
		result.show();
		// +--------------+
		// |average_salary|
		// +--------------+
		// |        3750.0|
		// +--------------+
	}
	
	private static void applyAccumulator(){
		LongAccumulator accum = sc.sc().longAccumulator();
		sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
		// ...
		// 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s
		accum.value();
		// returns 10
	}
	
	private static void applyFunction1(){
			JavaRDD<String> lines = sc.textFile("data.txt");
			JavaRDD<Integer> lineLengths = lines.map(new GetLength());
			int totalLength = lineLengths.reduce(new Sum());
	}
	
	private static void applyPair(){
		JavaRDD<String> lines = sc.textFile("data.txt");
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
		
		counts.sortByKey();
		
		counts.collect();
		
		counts.saveAsTextFile("hdfs://172.18.254.106:9000/output/pair_result.out");
	}
	
	private static void applyDataFrame1(){
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		Dataset<Row> df = spark.read().json("examples/src/main/resources/people.json");

		// Displays the content of the DataFrame to stdout
		df.show();
		// +----+-------+
		// | age|   name|
		// +----+-------+
		// |null|Michael|
		// |  30|   Andy|
		// |  19| Justin|
		// +----+-------+
		// Print the schema in a tree format
		df.printSchema();
		// root
		// |-- age: long (nullable = true)
		// |-- name: string (nullable = true)

		// Select only the "name" column
		df.select("name").show();
		// +-------+
		// |   name|
		// +-------+
		// |Michael|
		// |   Andy|
		// | Justin|
		// +-------+

		// Select everybody, but increment the age by 1
		df.select(col("name"), col("age").plus(1)).show();
		// +-------+---------+
		// |   name|(age + 1)|
		// +-------+---------+
		// |Michael|     null|
		// |   Andy|       31|
		// | Justin|       20|
		// +-------+---------+

		// Select people older than 21
		df.filter(col("age").gt(21)).show();
		// +---+----+
		// |age|name|
		// +---+----+
		// | 30|Andy|
		// +---+----+

		// Count people by age
		df.groupBy("age").count().show();
		// +----+-----+
		// | age|count|
		// +----+-----+
		// |  19|    1|
		// |null|    1|
		// |  30|    1|
		// +----+-----+
		
		// Register the DataFrame as a SQL temporary view
		df.createOrReplaceTempView("people");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();
		// +----+-------+
		// | age|   name|
		// +----+-------+
		// |null|Michael|
		// |  30|   Andy|
		// |  19| Justin|
		// +----+-------+
		
		// Register the DataFrame as a global temporary view
		try {
			df.createGlobalTempView("people");
		} catch (AnalysisException e) {
			e.printStackTrace();
		}

		// Global temporary view is tied to a system preserved database `global_temp`
		spark.sql("SELECT * FROM global_temp.people").show();
		// +----+-------+
		// | age|   name|
		// +----+-------+
		// |null|Michael|
		// |  30|   Andy|
		// |  19| Justin|
		// +----+-------+

		// Global temporary view is cross-session
		spark.newSession().sql("SELECT * FROM global_temp.people").show();
		// +----+-------+
		// | age|   name|
		// +----+-------+
		// |null|Michael|
		// |  30|   Andy|
		// |  19| Justin|
		// +----+-------+
	}
	
	private static void applyParquetFile(){
		SparkSession spark = SparkSession.builder().appName("SparkSQL APP").getOrCreate();
		
		Dataset<Row> peopleDF = spark.read().json("examples/src/main/resources/people.json");

		// DataFrames can be saved as Parquet files, maintaining the schema information
		peopleDF.write().parquet("people.parquet");

		// Read in the Parquet file created above.
		// Parquet files are self-describing so the schema is preserved
		// The result of loading a parquet file is also a DataFrame
		Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");

		// Parquet files can also be used to create a temporary view and then used in SQL statements
		parquetFileDF.createOrReplaceTempView("parquetFile");
		Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
		Dataset<String> namesDS = namesDF.map(
		    (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
		    Encoders.STRING());
		namesDS.show();
		// +------------+
		// |       value|
		// +------------+
		// |Name: Justin|
		// +------------+
	}
	
	static class Square implements Serializable {
		private static final long serialVersionUID = 1L;
		private int value;
		private int square;
		public int getValue() {
			return value;
		}
		public void setValue(int value) {
			this.value = value;
		}
		public int getSquare() {
			return square;
		}
		public void setSquare(int square) {
			this.square = square;
		}
	}

	static class Cube implements Serializable {
		private static final long serialVersionUID = 1L;
		private int value;
		private int cube;
		public int getValue() {
			return value;
		}
		public void setValue(int value) {
			this.value = value;
		}
		public int getCube() {
			return cube;
		}
		public void setCube(int cube) {
			this.cube = cube;
		}
	}

	
	private static void testSchemaMerge(){
		SparkSession spark = SparkSession.builder().appName("SparkSQL APP").getOrCreate();
		
		List<Square> squares = new ArrayList<>();
		for (int value = 1; value <= 5; value++) {
		  Square square = new Square();
		  square.setValue(value);
		  square.setSquare(value * value);
		  squares.add(square);
		}

		// Create a simple DataFrame, store into a partition directory
		Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
		squaresDF.write().parquet("data/test_table/key=1");

		List<Cube> cubes = new ArrayList<>();
		for (int value = 6; value <= 10; value++) {
		  Cube cube = new Cube();
		  cube.setValue(value);
		  cube.setCube(value * value * value);
		  cubes.add(cube);
		}

		// Create another DataFrame in a new partition directory,
		// adding a new column and dropping an existing column
		Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
		cubesDF.write().parquet("data/test_table/key=2");

		// Read the partitioned table
		Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
		mergedDF.printSchema();

		// The final schema consists of all 3 columns in the Parquet files together
		// with the partitioning column appeared in the partition directory paths
		// root
		//  |-- value: int (nullable = true)
		//  |-- square: int (nullable = true)
		//  |-- cube: int (nullable = true)
		//  |-- key: int (nullable = true)
	}
	
	private static void applyJSONDatasets(){
		SparkSession spark = SparkSession.builder().appName("SparkSQL APP").getOrCreate();
		
		// A JSON dataset is pointed to by path.
		// The path can be either a single text file or a directory storing text files
		Dataset<Row> people = spark.read().json("examples/src/main/resources/people.json");

		// The inferred schema can be visualized using the printSchema() method
		people.printSchema();
		// root
		//  |-- age: long (nullable = true)
		//  |-- name: string (nullable = true)

		// Creates a temporary view using the DataFrame
		people.createOrReplaceTempView("people");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
		namesDF.show();
		// +------+
		// |  name|
		// +------+
		// |Justin|
		// +------+

		// Alternatively, a DataFrame can be created for a JSON dataset represented by
		// a Dataset<String> storing one JSON object per string.
		List<String> jsonData = Arrays.asList(
		        "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
		anotherPeople.show();
		// +---------------+----+
		// |        address|name|
		// +---------------+----+
		// |[Columbus,Ohio]| Yin|
		// +---------------+----+
	}
	
	static class Record implements Serializable {
		private static final long serialVersionUID = 1L;
		private int key;
		private String value;

		public int getKey() {
			return key;
		}

		public void setKey(int key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}
	
	private static void applyHiveTables(){
		// warehouseLocation points to the default location for managed databases and tables
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		SparkSession spark = SparkSession
		  .builder()
		  .appName("Java Spark Hive Example")
		  .config("spark.sql.warehouse.dir", warehouseLocation)
		  .enableHiveSupport()
		  .getOrCreate();

		spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
		spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");

		// Queries are expressed in HiveQL
		spark.sql("SELECT * FROM src").show();
		// +---+-------+
		// |key|  value|
		// +---+-------+
		// |238|val_238|
		// | 86| val_86|
		// |311|val_311|
		// ...

		// Aggregation queries are also supported.
		spark.sql("SELECT COUNT(*) FROM src").show();
		// +--------+
		// |count(1)|
		// +--------+
		// |    500 |
		// +--------+

		// The results of SQL queries are themselves DataFrames and support all normal functions.
		Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

		// The items in DataFrames are of type Row, which lets you to access each column by ordinal.
		Dataset<String> stringsDS = sqlDF.map(
		    (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
		    Encoders.STRING());
		stringsDS.show();
		// +--------------------+
		// |               value|
		// +--------------------+
		// |Key: 0, Value: val_0|
		// |Key: 0, Value: val_0|
		// |Key: 0, Value: val_0|
		// ...

		// You can also use DataFrames to create temporary views within a SparkSession.
		List<Record> records = new ArrayList<>();
		for (int key = 1; key < 100; key++) {
		  Record record = new Record();
		  record.setKey(key);
		  record.setValue("val_" + key);
		  records.add(record);
		}
		Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
		recordsDF.createOrReplaceTempView("records");

		// Queries can then join DataFrames data with data stored in Hive.
		spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
		// +---+------+---+------+
		// |key| value|key| value|
		// +---+------+---+------+
		// |  2| val_2|  2| val_2|
		// |  2| val_2|  2| val_2|
		// |  4| val_4|  4| val_4|
		// ...
	}
	
	private static void applyJDBCDatasource(){
		SparkSession spark = SparkSession.builder().appName("SparkSQL APP").getOrCreate();
				
		// Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
		// Loading data from a JDBC source
		Dataset<Row> jdbcDF = spark.read()
		  .format("jdbc")
		  .option("url", "jdbc:postgresql:dbserver")
		  .option("dbtable", "schema.tablename")
		  .option("user", "username")
		  .option("password", "password")
		  .load();

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "username");
		connectionProperties.put("password", "password");
		Dataset<Row> jdbcDF2 = spark.read()
		  .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

		// Saving data to a JDBC source
		jdbcDF.write()
		  .format("jdbc")
		  .option("url", "jdbc:postgresql:dbserver")
		  .option("dbtable", "schema.tablename")
		  .option("user", "username")
		  .option("password", "password")
		  .save();

		jdbcDF2.write()
		  .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

		// Specifying create table column data types on write
		jdbcDF.write()
		  .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
		  .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
	}

	public static void main(String[] args) {
		String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
	    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
	    Dataset<String> logData = spark.read().textFile(logFile).cache();

	    FilterFunction<String> funcA = new FilterFunction<String>(){
			@Override
			public boolean call(String value) {
				return value.contains("a");
			}
		};

		FilterFunction<String> funcB = new FilterFunction<String>(){
			@Override
			public boolean call(String value) {
				return value.contains("b");
			}
		};
	    long numAs = logData.filter(funcA).count();
	    long numBs = logData.filter(funcB).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

	    spark.stop();
	}

}
