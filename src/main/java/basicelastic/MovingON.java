package basicelastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.collection.JavaConverters;

import javax.xml.bind.DatatypeConverter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;



public class MovingON implements Serializable {

    public String rowJsonStr(Row row){
        scala.collection.Map<String,String> map = row. getValuesMap(JavaConverters.asScalaIteratorConverter(Arrays.asList(row.schema().fieldNames()).iterator()).asScala().toSeq());
        Map<String,String> javaMap = new HashMap<>();
        scala.collection.Set keys =  map.keySet();
        scala.collection.Iterator<String> it = keys.iterator();
        while (it.hasNext()){
            String key = it.next();
            scala.Option<String> option = map.get(key);
            if(option.nonEmpty()){
                javaMap.put(key,option.get());
            }
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String serialziedAsJsonString = objectMapper.writeValueAsString(javaMap);
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            String id = DatatypeConverter.printHexBinary(md5.digest(serialziedAsJsonString.getBytes(StandardCharsets.UTF_8))).toLowerCase(Locale.ROOT);
            javaMap.put("id",id);
            return  objectMapper.writeValueAsString(javaMap);
        }catch(Exception ex){
            ex.printStackTrace();
        }
        throw new RuntimeException("Failed to encode as json string");
    }
    public void run(){
        SparkSession spark = SparkSession
                .builder()
                .appName("some app").
                enableHiveSupport()
                .getOrCreate();
        SQLContext sql = new SQLContext(spark);
        Dataset<Row> dset = sql.sql("select * from somedb.sometable");
        dset.show();
        String forTest = rowJsonStr(dset.first());
        System.out.println("------------------------   forTest ---------------------------------" + forTest);
        Dataset<String> dsetStrings = dset.map((MapFunction<Row, String>) row -> {
            return rowJsonStr(row);
        }, Encoders.STRING());
        dsetStrings.show();
        Map<String,String> map = new HashMap<String,String>();
        map.put("es.nodes","localhost:9200");
        map.put("es.nodes.discovery","false");
        map.put("es.mapping.routing","type");
        map.put("es.write.operation","upsert");
        map.put("es.mapping.id","id");
        map.put("es.mapping.version.type","external");




        JavaEsSpark.saveJsonToEs(dsetStrings.toJavaRDD(),"alon-first/doc",map);
    }
    public final static void main(String args[]) throws NoSuchAlgorithmException {
        System.out.println("Alon");
        new MovingON().run();
    }
}
