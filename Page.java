import java.io.Serializable;
import java.util.*;
import java.util.function.UnaryOperator;

public class Page implements Serializable {
    Vector<Hashtable<String,Object>> list;
    int listNumber;
    String PageID;
    String cluster;
    Vector<String> clusterings;
    transient Page overflowPage ;
    public Page(String pageName) {
        list = new Vector<Hashtable<String,Object>>();
        clusterings=new Vector<String>();
        PageID = pageName;


    }


    public int compare(Hashtable<String, Object> a, Hashtable<String, Object> b) {
        return (a.get(cluster).toString().compareTo(b.get(cluster).toString()));
    }


}