import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

public class GenerateCsv {
    public static void main(String args[]) throws IOException {
        String basedName = "partition_csv_";
        int rows = 5;
        int partitions = 4;
        List<String> types = Arrays.asList("writer","doctor","cleaner");
        for( int i = 0 ; i < partitions; i++){
            FileWriter fw = new FileWriter("/home/alon/tmp/"+basedName+i);
            for (int j = 0; j < rows; j++) {
                StringBuffer buf = new StringBuffer();
                buf.append("moshe").append(j).append(',');
                buf.append(types.get(j % types.size())).append(',');
                buf.append(j*j);
                buf.append('\n');
                fw.write(buf.toString());
            }

            fw.close();
        }

    }
}
