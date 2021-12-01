package basicelastic;
import java.io.Serializable;
import java.util.Map;

public class WrapHmap implements Serializable {
    final public Map<String,String> map;
    public WrapHmap(Map<String,String> map){
        this.map = map;
    }
}
