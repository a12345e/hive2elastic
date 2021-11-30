package basicelastic;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.rdd.EsSpark;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

public class Basic {
    public static void main(String[] args) {
         SparkSession spark = SparkSession
                .builder()
                .appName("some app").
                enableHiveSupport()
                .getOrCreate();
        SQLContext sql = new SQLContext(spark);
        Dataset<Row> dset = sql.sql("select * from somedb.sometable");
        dset.show();
        Map<String,String> map = new HashMap<String,String>();
        map.put("es.nodes","localhost:9200");
        map.put("es.nodes.discovery","false");
        map.put("es.mapping.routing","type");
        map.put("es.write.operation","upsert");

        List<Tuple2<String, String>> tuples = map.entrySet().stream()
                .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        scala.collection.Map scalaMap = scala.collection.Map$.MODULE$.apply(JavaConversions.asScalaBuffer(tuples).toSeq());
        EsSparkSQL.saveToEs(dset,"alon-first/doc",scalaMap);
        //EsSpark.saveJsonToEs(dset,"alon-index-first/doc",scalaMap);
//        spark.close();
//        System.out.println("Alon had good run");
//        System.err.println("Alon had bad run");

    }


}
