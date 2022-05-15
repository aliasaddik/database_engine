import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Index implements Serializable {
    String tableName;
    String clusteringKey ;
    String[] colNames;
    Hashtable<String,Vector<String>> ranges;
    String indexId;
    Vector grid;
    public Index(String tableName, String[] columnNames ){
        this.tableName=tableName;
        colNames=columnNames;
        grid = new Vector();
        clusteringKey= clustering();
        indexId="IDX_"+tableName;
        for (int i=0;i<columnNames.length;i++){
            indexId+="_"+columnNames[i];
        }
    ranges=new Hashtable<String,Vector<String>>();
        for (int i=0;i<colNames.length;i++)
            ranges.put(columnNames[i],new Vector());
    }
    public  void serializeIndex(){
        String indexId = this.indexId;
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+indexId+".bin");
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
    public static Index DeserializeIndex(String path){
        Index i = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            i = (Index) in.readObject();
            in.close();
            file.close();

            //  System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage());
        }
        return i;}
    public  String clustering()  {
        Table target = null;
        String tableFilePath = null;
        Vector tables = DBApp.deserializeVector("src/main/resources/data/tablesList.bin");
        Vector<String> tableNames = DBApp.deserializeVector("src/main/resources/data/tableNames.bin");
        //getting targetted table
        for (int i = 0; i < tables.size(); i++) {
            if (tableNames.get(i).compareTo(tableName) == 0) {
                tableFilePath = (String) tables.get(i);
                target = DBApp.DeserializeTable(tableFilePath);
                break;
            }
        }

        return target.clusteringKey.toString();
    }

    public Vector<String>  search(Vector<Vector<Integer>> indices){// the small vector has the locations of the cells I want
        Vector<String> bucket_Paths =new Vector<String>();
        for(int i=0 ;i<indices.size();i++){
            Vector<Integer> location=indices.get(i);//[1,0,0]

            bucket_Paths.add((String) helper(grid,location));

        }
        return bucket_Paths;
    }


    public Object helper(Vector<Vector> subgrid, Vector<Integer> location){
        if(location.size()==1){
            return subgrid.get(location.get(0));
            //[[1,2],[1,3]]
        }
        else{
            location.removeElementAt(0);
            return helper(subgrid.get(location.get(0)),location);
        }

    }
    }


