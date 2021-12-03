package basicelastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.collection.JavaConverters;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;


public class MovingON implements Serializable {
    public List<String> asIfItIsHamster(String tableRowInJsonFormat) throws IOException, NoSuchAlgorithmException {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String,Object> result =
                new ObjectMapper().readValue(tableRowInJsonFormat, Map.class);
        String serialziedAsJsonString = objectMapper.writeValueAsString(result);
       MessageDigest md5 = MessageDigest.getInstance("MD5");
       String id = DatatypeConverter.printHexBinary(md5.digest(serialziedAsJsonString.getBytes(StandardCharsets.UTF_8))).toLowerCase(Locale.ROOT);
       result.put("id",id);
       return Arrays.asList(objectMapper.writeValueAsString(result));
    }

    public String prepareJsonToHamster(Row row){
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
        Dataset<String> strings4Hamster = dset.map((MapFunction<Row, String>) row -> {
            return prepareJsonToHamster(row);
        }, Encoders.STRING());
        strings4Hamster.show();
        System.out.println("This is to be sent to Hamster");
        Dataset<String> hamsterResults = strings4Hamster.flatMap((FlatMapFunction<String, String>)
               x ->  { return asIfItIsHamster(x).iterator() ;} ,Encoders.STRING());
        Map<String,String> map = new HashMap<String,String>();
        map.put("es.nodes","localhost:9200");
        map.put("es.nodes.discovery","false");
        map.put("es.mapping.routing","type");
        map.put("es.write.operation","upsert");
        map.put("es.mapping.id","id");
        map.put("es.mapping.version.type","external");
        JavaEsSpark.saveJsonToEs(hamsterResults.toJavaRDD(),"alon-{type}/doc",map);
    }
    public final static void main(String args[]) throws NoSuchAlgorithmException {
        System.out.println("Alon");
        new MovingON().run();
    }
}
