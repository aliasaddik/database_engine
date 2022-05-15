import java.io.*;
import java.util.*;

public class Bucket implements Serializable {
    int noOfEntries;
    int max;
    HashMap<String, Vector<String>> list ;
    String BucketId;
    Bucket overFlow ;
    String indexPath;
    public Bucket(String id , String i) {
        noOfEntries= 0;
        try {
            max = readConfig("MaximumKeysCountinIndexBucket");
        } catch (IOException e) {
            e.printStackTrace();
        }
        BucketId=id+"bucket";
        list = new HashMap<String,Vector<String>>();
        this.indexPath=i;
    }
    public Vector<String> search(String s){
        Vector<String> path = new Vector<String>();
        path  =list.get(s);
    return path;}
    static Vector<Object> a ;


/*public void insert(String tableName) {
    Table t = getTable(tableName);
    for (int i = 0; i < t.pagesPath.size(); i++) {
        Page p = DBApp.deserialize(t.pagesPath.get(i));
        for (int j = 0; j < p.list.size(); j++) {
            Vector <String> paths= new Vector();
            Set<String> columns = min.keySet();
            for (String k : columns) {
                if(p.list.get(j).get(k).toString().compareTo(min.get(k))>0 && p.list.get(j).get(k).toString().compareTo(max.get(k))<0){
                    paths.add(t.pagesPath.get(i));
                    list.put(p.list.get(j).get(t.clusteringKey).toString(),paths);
                }
            }

        }
    }
} */
public Table getTable(String tableName){
      Vector<String> tables = null;
      Vector<String > tableNames =DBApp.deserializeVector("src/main/resources/data/tableNames.bin");
      String tableFilePath="";
      Table t = null;
      tables = DBApp.deserializeVector("src/main/resources/data/tablesList.bin");
      for(int i =0 ;i<tables.size();i++){
          if (tableNames.get(i).compareTo(tableName) == 0) {
              tableFilePath = tables.get(i);
              t = DBApp.DeserializeTable(tableFilePath);
              break;
          }
      }
return t;}
    public  void serializeBucket(){
        String bucketId = this.BucketId;
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+bucketId+".bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(this);

            out.close();
            file.close();
            //   System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage());
        }

    }
    public static Bucket DeserializeBucket(String path){
        Bucket b = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            b = (Bucket) in.readObject();
            in.close();
            file.close();

            //  System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage());
        }
        return b;}
    public int readConfig(String key) throws IOException {
        int nRows=0;
        FileReader reader=new FileReader("src/main/resources/DBApp.config");

        Properties p=new Properties();
        p.load(reader);
        return Integer.parseInt(p.getProperty(key));
    }


}

