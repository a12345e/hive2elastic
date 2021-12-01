package basicelastic;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

    public class DatasetToSeq implements Serializable {
    void run(){
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("some app").
                getOrCreate();
        //SQLContext sql = new SQLContext(spark);
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("/tmp/alon/partition_csv_0", 1)
                .toJavaRDD();
        // The schema is encoded in a string
        String schemaString = "name type age";

// Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        Arrays.stream(schema.fieldNames()).forEach(x->System.out.print(x));

        JavaRDD<Row> rowRDD = peopleRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String record) throws Exception {
                String[] attributes = record.split(",");
                return RowFactory.create(attributes[0],attributes[1],attributes[2]);
            }
        });
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
        peopleDataFrame.show();


        //final Encoder<WrapHmap> wrapHmap = Encoders.bean(WrapHmap.class);
        final Encoder<WrapHmap> wrapHmap = Encoders.javaSerialization(WrapHmap.class);

        Dataset<WrapHmap> maps = peopleDataFrame.map((MapFunction<Row, WrapHmap>) row -> {
            int length = schema.fieldNames().length;
            Map<String,String> map = new HashMap<>();
            for(int i=0; i < length; i++){
                StructField structField = schema.fields()[i];
                String name = structField.name();
                DataType dataType = structField.dataType();
                Object obj = row.get(i);
                String value;
                if(obj.getClass() == Integer.class){
                    Integer intValue = (Integer) obj;
                    value = intValue.toString();
                }else if(obj.getClass() == Long.class){
                    Long longValue = (Long) obj;
                    value = longValue.toString();
                }else {
                    value = (String)obj;
                }
                map.put(name,value);
            }
            map.put("id",computeId(map));
            return new WrapHmap(map);
        },wrapHmap);
        //dset.show();
        maps.show();
        spark.close();
    }
    public static void main(String[] args) {
        new DatasetToSeq().run();
    }
     public String computeId(Map<String,String> map){
        List<String> orderedKeys = map.keySet().stream().collect(Collectors.toList());
        Collections.sort(orderedKeys);
        StringBuffer id = new StringBuffer();
        for(String field: orderedKeys){
            id.append(field);
            id.append('=');
            id.append(map.get(field));
            id.append(',');
        }
        return id.substring(0,id.length()-1);
    }
//
//    static public scala.collection.mutable.HashMap<String,String> mapRow2Hmap(Row row){
//        scala.collection.mutable.HashMap<String, String> hmap = new scala.collection.mutable.HashMap<String, String>();
//        scala.collection.Map<String,String> map = row. getValuesMap(JavaConverters.asScalaIteratorConverter(Arrays.asList(row.schema().fieldNames()).iterator()).asScala().toSeq());
//        scala.collection.Set keys =  map.keySet();
//        scala.collection.Iterator<String> it = keys.iterator();
//        while (it.hasNext()){
//            String key = it.next();
//            scala.Option<String> option = map.get(key);
//            if(option.nonEmpty()){
//                hmap.put(key,option.get());
//            }
//        }
//        return hmap;
//    }
}
