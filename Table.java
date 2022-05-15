import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
    String tableName;
    Vector<String> pagesPath;
    Vector<String> min ;
    Vector <String>max;
    String clusteringKey;
    Vector<String> colName;
    Vector<String> indicies;
    public Table (String name,String clustering){
        tableName = name;
        pagesPath = new Vector<String>();
        clusteringKey = clustering;
        min = new Vector<String>();
        max= new Vector<String>();
        indicies=new Vector<String>();


    }
}
