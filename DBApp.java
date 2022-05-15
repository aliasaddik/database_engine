import org.junit.jupiter.api.parallel.Resources;
import org.junit.validator.ValidateWith;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map;
///Final
public class DBApp implements DBAppInterface {

    //private static Object String;
    Vector<String> tableList;
    Vector<String> tableNames;
    // Vector<Index> indexes;
    public Vector<SQLTerm> conditionsOfIndexedColumns;
    private static final String FOLDER ="src/main/resources/data";


    public DBApp (){
        tableList= new Vector<String>();
        tableNames = new Vector<String>();

    }

    @Override
    public void init() {
        FileWriter csvWriter = null;
        //File data = new File("src/main/resources");
        File newFolder = new File(FOLDER);

        boolean created =  newFolder.mkdir();

        if(created)
            System.out.println("Folder was created !");
        else
            System.out.println("sh");

        String title = "Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max";
        try {
            csvWriter = new FileWriter("src/main/resources/metadata.csv", true);

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            csvWriter.write(title);
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        /*try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/Tables.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(tableList);
            out.close();
            file.close();

            System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }*/
    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String,String> colNameType,
                            Hashtable<String,String> colNameMin, Hashtable<String,String> colNameMax) throws DBAppException, IOException {
        Vector<String> colName=new Vector<>();
        try {
            String columnName = "";
            Set<String> columns = colNameType.keySet();
            for (String k : columns) {
                colName.add(k);
                columnName = k;
                if (!(colNameMax.containsKey(k) && colNameMin.containsKey(k)))
                    throw new DBAppException("invalid input");

            }
            FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
            csvWriter.write(getAttributes(tableName, clusteringKey, colNameType, colNameMin, colNameMax));
            csvWriter.flush();
            csvWriter.close();
            Table table = new Table(tableName, clusteringKey);
            Vector <String> tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
            if(tableNames.contains(tableName))
                throw new DBAppException("A table with this name already exists");
            Vector <String> tableList = deserializeVector("src/main/resources/data/tablesList.bin");
            tableList.add("src/main/resources/data/"+tableName+".bin");
            serializetableList(tableList);

            tableNames.add(tableName);
            serializetableNames(tableNames);
            serializeTable(table);

        }
        catch(Exception e){
            System.out.print(e.getMessage());
        }
    /*    FileOutputStream fout=new FileOutputStream("src/main/resources/metadata.csv");
        String data =getAttributes(tableName,clusteringKey,colNameType,colNameMin,colNameMax);
        fout.write(data.getBytes(), 0, data.length());
*/
    }
    /*    FileOutputStream fout=new FileOutputStream("src/main/resources/metadata.csv");
        String data =getAttributes(tableName,clusteringKey,colNameType,colNameMin,colNameMax);
        fout.write(data.getBytes(), 0, data.length());
*/
    // @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {


        // System.out.println("I came not update");
        Index index = new Index(tableName, columnNames);
        Table table = DeserializeTable("src/main/resources/data/" + tableName + ".bin");
        // Vector indicies = deserializeVector("src/main/resources/data/indicies.bin");
        table.indicies.add("src/main/resources/data/" + index.indexId + ".bin");
        serializeTable(table);
        buildArray(index.colNames.length, index.grid);
        System.out.println(index.grid);
        updateMetadata(columnNames, tableName);
        createRanges(index);
        //method salma w nouran
        loopPages(tableName, columnNames, index);
        //System.out.println("I came loop");
        //    indexes.add(index);??
        index.serializeIndex();
    }




    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

        colNotFound(tableName, colNameValue);
        Vector<String> tables = null; //all tables in the DB
        Table target = null;
        String tableFilePath = null;
        String currentClustering = null; //clusteringKey of the entry
        int maxRows = 0;
        try {
            maxRows = readConfig("MaximumRowsCountinPage");
        } catch (IOException e) {
            e.printStackTrace();
        }
        tables = deserializeVector("src/main/resources/data/tablesList.bin");
        Vector<String> tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
        //getting targetted table
        for (int i = 0; i < tables.size(); i++) {
            if (tableNames.get(i).compareTo(tableName) == 0) {
                tableFilePath = tables.get(i);
                target = DeserializeTable(tableFilePath);
                break;
            }
        }
        if (target == null)
            throw new DBAppException("Table not found");
        currentClustering = colNameValue.get(target.clusteringKey).toString();
        if (currentClustering == null) {
            throw new DBAppException("you entered an entry with no primary key");
        }
        //table has no pages case
        if (target.pagesPath.size() == 0) {
            Page page = new Page(tableName + "0");
            try {
                checkDataTypes(tableName, colNameValue);
            }
            catch (Exception e){
                System.out.println(e.getMessage());
            }
            page.list.add(colNameValue);
            page.clusterings.add(colNameValue.get(target.clusteringKey).toString());
            serializePage(page);
            //"src/main/resources/data/"+pageName+".bin"
            target.pagesPath.add("src/main/resources/data/" + page.PageID + ".bin");
            target.min.add(colNameValue.get(target.clusteringKey).toString());
            target.max.add(colNameValue.get(target.clusteringKey).toString());
            serializeTable(target);
            serializePage(page);

        }//end of no pages case
        else {
            //finding the target page
            int pageIndex = 0;
            int k;
            for (k = 0; k < target.pagesPath.size(); k++) {
                if (currentClustering.compareTo(target.min.get(k).toString()) < 0) {
                    String currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                    Page currentPage = deserialize(currentPath);
                    try {
                        if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                            throw new DBAppException("there is an entry with this primary Key !");
                    }
                    catch (Exception ex){
                        System.out.println(ex.getMessage());
                    }
                    if (currentPage.list.size() < maxRows) {
                        insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                        serializeTable(target);
                        serializePage(currentPage);
                        return;
                    }
                    //I reached max rows
                    else {
                        // check for existance of next page should be added
                        //I have a next page case
                        String nextPath = "src/main/resources/data" + "/" + target.tableName + (k + 1) + ".bin";
                        if (target.pagesPath.contains(nextPath)) {
                            Page nextPage = deserialize(nextPath);
                            // next page below max rows
                            try{
                                if (nextPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                    throw new DBAppException("there is an entry with this primary Key !");}
                            catch(Exception e){
                                System.out.println(e.getMessage());
                            }
                            if (nextPage.list.size() < maxRows) {
                                Hashtable targetElement = currentPage.list.get(currentPage.list.size() - 1);
                                currentPage.list.removeElementAt(currentPage.list.size() - 1);
                                String newMax = currentPage.list.get(currentPage.list.size() - 1).get(target.clusteringKey).toString();
                                target.max.set(k, newMax);
                                currentPage.clusterings.removeElementAt(currentPage.clusterings.size() - 1);
                                nextPage.list.insertElementAt(targetElement, 0);
                                nextPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(), 0);
                                target.min.set(k + 1, (targetElement.get(target.clusteringKey)).toString());
                                insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                                serializePage(nextPage);
                                serializePage(currentPage);
                                serializeTable(target);
                            } else {
                                currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                                currentPage = deserialize(currentPath);
                                try {
                                    if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                        throw new DBAppException("there is an entry with this primary Key !");
                                }
                                catch (Exception e){
                                    System.out.println(e.getMessage());
                                }
                                if (currentPage.overflowPage == null)
                                    currentPage.overflowPage = new Page(currentPage.PageID + "Over");
                                insertIntoPage(currentPage.overflowPage, colNameValue, currentClustering, target, k);
                                serializePage(currentPage.overflowPage);
                                serializePage(currentPage);
                                serializeTable(target);
                                return;
                            }
                        }//I dont have nextPage
                        // in CASE THAT THE CURRENT PAGE IS THE LAST PAGE , THEN WE CREATE NEW PAGE AND SHIFT ONE ROW DOWN
                        else {
                            Page newPage = new Page(target.tableName + target.pagesPath.size());
                            target.pagesPath.add("src/main/resources/data" + "/" + newPage.PageID + ".bin");
                            currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                            currentPage = deserialize(currentPath);
                            if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                throw new DBAppException("there is an entry with this primary Key !");
                            Hashtable targetElement = currentPage.list.get(currentPage.list.size() - 1);
                            currentPage.list.removeElementAt(currentPage.list.size() - 1);
                            String newMax = currentPage.list.get(currentPage.list.size() - 1).get(target.clusteringKey).toString();
                            target.max.set(k, newMax);
                            currentPage.clusterings.removeElementAt(currentPage.clusterings.size() - 1);
                            newPage.list.insertElementAt(targetElement, 0);
                            newPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(), 0);
                            target.min.add(targetElement.get(target.clusteringKey).toString());
                            insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                            target.max.add(targetElement.get(target.clusteringKey).toString());
                            serializePage(newPage);
                            serializePage(currentPage);
                            serializeTable(target);
                            return ;
                        }
                        //end of else -Iam not below max rows-
                    }
                }//end of I am below min case
                else{
                    if (currentClustering.compareTo(target.min.get(k).toString()) > 0 && currentClustering.compareTo(target.max.get(k).toString()) < 0) {
                        String currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                        Page currentPage = deserialize(currentPath);
                        try{
                            if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                throw new DBAppException("there is an entry with this primary Key !");}
                        catch (Exception e){
                            System.out.println(e.getMessage());
                        }
                        if (currentPage.list.size() < maxRows) {
                            insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                            serializeTable(target);
                            serializePage(currentPage);
                            return;
                        }//No I reached max
                        else {
                            // check for existance of next page should be added
                            //I have a next page case
                            String nextPath = "src/main/resources/data" + "/" + target.tableName + (k + 1) + ".bin";
                            if (target.pagesPath.contains(nextPath)) {
                                Page nextPage = deserialize(nextPath);
                                // next page below max rows
                                try {
                                    if (nextPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                        throw new DBAppException("there is an entry with this primary Key !");
                                }
                                catch(Exception e){
                                    System.out.println(e.getMessage());
                                }
                                if (nextPage.list.size() < maxRows) {
                                    Hashtable targetElement = currentPage.list.get(currentPage.list.size() - 1);
                                    currentPage.list.removeElementAt(currentPage.list.size() - 1);
                                    String newMax = currentPage.list.get(currentPage.list.size() - 1).get(target.clusteringKey).toString();
                                    target.max.set(k, newMax);
                                    currentPage.clusterings.removeElementAt(currentPage.clusterings.size() - 1);
                                    nextPage.list.insertElementAt(targetElement, 0);
                                    nextPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(), 0);
                                    target.min.set(k + 1, targetElement.get(target.clusteringKey).toString());
                                    insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                                    serializePage(nextPage);
                                    serializePage(currentPage);
                                    serializeTable(target);
                                } else {
                                    currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                                    currentPage = deserialize(currentPath);
                                    try{
                                        if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                            throw new DBAppException("there is an entry with this primary Key !");
                                    }
                                    catch(Exception e){
                                        System.out.println(e.getMessage());
                                    }
                                    if (currentPage.overflowPage == null)
                                        currentPage.overflowPage = new Page(currentPage.PageID + "Over");
                                    insertIntoPage(currentPage.overflowPage, colNameValue, currentClustering, target, k);
                                    serializePage(currentPage.overflowPage);
                                    serializePage(currentPage);
                                    serializeTable(target);
                                    return;
                                }
                            }//I dont have nextPage
                            // in CASE THAT THE CURRENT PAGE IS THE LAST PAGE , THEN WE CREATE NEW PAGE AND SHIFT ONE ROW DOWN
                            else {
                                Page newPage = new Page(target.tableName + target.pagesPath.size());
                                target.pagesPath.add("src/main/resources/data" + "/" + newPage.PageID + ".bin");
                                currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                                currentPage = deserialize(currentPath);
                                if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                    throw new DBAppException("there is an entry with this primary Key !");
                                Hashtable targetElement = currentPage.list.get(currentPage.list.size() - 1);
                                currentPage.list.removeElementAt(currentPage.list.size() - 1);
                                String newMax = currentPage.list.get(currentPage.list.size() - 1).get(target.clusteringKey).toString();
                                target.max.set(k, newMax);
                                currentPage.clusterings.removeElementAt(currentPage.clusterings.size() - 1);
                                newPage.list.insertElementAt(targetElement, 0);
                                newPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(), 0);
                                target.min.add(targetElement.get(target.clusteringKey).toString());
                                insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                                target.max.add(targetElement.get(target.clusteringKey).toString());
                                serializePage(newPage);
                                serializePage(currentPage);
                                serializeTable(target);
                                return ;
                            }
                            //end of else -Iam not below max rows-
                        }
                    }
                }
            }//end of loop
            if (k >= target.pagesPath.size()) {
                String path = target.pagesPath.get(target.pagesPath.size() - 1);
                Page lastPage = deserialize(path);
                if (lastPage.list.size() < maxRows) {
                    insertIntoPage(lastPage,colNameValue,currentClustering,target,target.pagesPath.size()-1);
                    serializeTable(target);
                    return;
                }
                else {
                    Page newPage = new Page(tableName + k);
                    newPage.list.add(colNameValue);
                    newPage.clusterings.add(currentClustering);
                    target.max.add(currentClustering);
                    target.min.add(currentClustering);
                    target.pagesPath.add("src/main/resources/data/" + newPage.PageID + ".bin");
                    serializePage(newPage);
                    serializeTable(target);
                    return;
                }
            }

        }    }


    public static void insertIntoPage (Page currentPage, Hashtable colNameValue, String currentClustering, Table
            target,int pageIndex) throws DBAppException {
        int x = 0;
        int idx = Collections.binarySearch(currentPage.clusterings, colNameValue.get(target.clusteringKey).toString());
        if(currentPage ==null)
            System.out.println("ANA HENA AHO");
        if(currentPage.clusterings.size()!=currentPage.list.size()){ System.out.println(currentPage.clusterings.size() +"clusterings");
            System.out.println(currentPage.list.size()+"list");

            System.out.println(currentPage.clusterings.toString());
            System.out.println(currentPage.list.toString());}
        if (idx<0) {


            x = -1 - idx;
            System.out.println("XXXXXXXXX: " + x);
            if (x > 250) {
                throw new DBAppException("Ana bayez xX");
            }
            try {
                currentPage.list.insertElementAt(colNameValue, x);

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            //inserting the clustering key at the clusterings vector
            currentPage.clusterings.insertElementAt(colNameValue.get(target.clusteringKey.toString()).toString(), x);


            if (currentClustering.compareTo(target.min.get(pageIndex).toString()) < 0) {
                target.min.set(pageIndex, currentClustering);
            }
            if (currentClustering.compareTo(target.max.get(pageIndex).toString()) > 0)
                target.max.set(pageIndex, currentClustering);
        }
        else{
            System.out.println("Record already exists");
        }
        serializePage(currentPage);
        serializeTable(target);
    }


    @Override
    public void updateTable (String tableName, String
            clusteringKeyValue, Hashtable < String, Object > columnNameValue) throws DBAppException {

        colNotFound(tableName, columnNameValue);
        try {
            checkDataTypes(tableName, columnNameValue);
            int i;

            // String s="s/"
            Boolean flag = false;

            Table table = null;
            Vector<String> tableList = (Vector<String>) deserializeVector("src/main/resources/data/tablesList.bin");
            for (i = 0; i < tableList.size(); i++) {
                table = DeserializeTable("src/main/resources/data/" + tableName + ".bin");
                if (table.tableName.equals(tableName)) {
                    if (columnNameValue.get(table.clusteringKey) != null)
                        throw new DBAppException("You can't update clustering key !");
                    flag = true;
                    break;
                }


            }
            if (flag) {
                Boolean flagfoundpage = false;
                int idx = -1;
                Page page;
                int j;
                for (j = 0; j < table.min.size(); j++) {
                    if (clusteringKeyValue.compareTo((table.min.get(j)).toString()) >= 0 &&
                            clusteringKeyValue.compareTo((table.max.get(j)).toString()) <= 0) {
                        page = deserialize("src/main/resources/data" + "/" + table.tableName + (j + 1) + ".bin");
                        //idx = Collections.binarySearch(page.clusterings,clusteringKeyValue);
                        if (!page.clusterings.contains(clusteringKeyValue)) {
                            if (page.overflowPage != null) {
                                Page overflow = deserialize("src/main/resources/data" + "/" + table.tableName + (j + 1) + "Over" + ".bin");
                                if (overflow.clusterings.contains(clusteringKeyValue)) {
                                    String s = (String) (columnNameValue.keySet().toArray())[0];
                                    columnNameValue.replace(s, columnNameValue.get(s));
                                    flagfoundpage = true;
                                    System.out.println("updating in overFloe");

                                    break;
                                }
                            }
                        } else if (page.clusterings.contains(clusteringKeyValue)) {
                            String s = (String) (columnNameValue.keySet().toArray())[0];
                            columnNameValue.replace(s, columnNameValue.get(s));
                            flagfoundpage = true;
                        }

                        serializePage(page);
                        break;
                    }
                }
                if (!flagfoundpage) {
                    throw new DBAppException("Row not found");
                }

            } else {
                throw new DBAppException("The table does not exist");
            }
            serializeTable(table);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    @Override
    public void deleteFromTable (String tableName, Hashtable < String, Object > columnNameValue) throws
            DBAppException {

        String indexPath = getIndex(tableName, columnNameValue);
        if (indexPath == null) {
            Page p = getPage(tableName, columnNameValue);
            if (p != null) {
                try {
                    Table t = DeserializeTable("src/main/resources/data/" + tableName + ".bin");
                    int rowIdx = getRow(p, columnNameValue, tableName);
                    String cluster = p.list.get(rowIdx).get(t.clusteringKey).toString();
                    p.list.removeElementAt(rowIdx);
                    p.clusterings.removeElementAt(rowIdx);
                    char pid = p.PageID.charAt(tableName.length());
                    int idx = Character.getNumericValue(pid);
                    if (cluster.equals(t.min.get(idx))) {
                        t.min.set(idx, p.clusterings.get(0));
                    }

                    if (cluster.equals(t.max.get(idx))) {
                        t.max.removeElementAt(idx);
                        t.max.set(idx, p.clusterings.get(p.clusterings.size() - 1));
                    }
                    serializeTable(t);
                } catch (Exception x) {
                    System.out.println(x.getMessage());
                }
            }
        }else {
            // DELETING USING THE INDEX
            Index index =  Index.DeserializeIndex(indexPath);
            Hashtable<String,Object> existingCol =new Hashtable<String,Object>();
            Hashtable<String,Object> nonExistingCol =new Hashtable<String,Object>();

            // identifying existing cols in the delete request compared to index columns
            for (int k=0; k<index.colNames.length;k++){
                if (columnNameValue.containsKey(index.colNames[k]))
                    existingCol.put(index.colNames[k],columnNameValue.get(index.colNames[k]));
            }

            // identifying nonExisting cols in the index compared to the delete request

            for (Map.Entry<String, Object> e : columnNameValue.entrySet()){
                if (!containsKey(index.colNames,e.getKey()))
                    nonExistingCol.put(e.getKey(),e.getValue());
            }


            deleteUsingIndex(index,existingCol,nonExistingCol,0,index.grid,columnNameValue);



        }
    }

    //@Override
    // public Iterator selectFromTable (SQLTerm[]sqlTerms, String[]arrayOperators) throws DBAppException {
    //  return null;
    // }

    public static String getAttributes (String tableName, String
            clusteringKey, Hashtable < String, String > colNameType,
                                        Hashtable < String, String > colNameMin, Hashtable < String, String > colNameMax){
        String columnName = "";
        String columnType = "";
        String columnMin = "";
        String columnMax = "";
        String clustering;
        String result = "";
        Set<String> columns = colNameType.keySet();
        for (String k : columns) {

            columnName = k;
            columnType = colNameType.get(k);
            columnMax = colNameMax.get(k);
            columnMin = colNameMin.get(k);
            if (clusteringKey.equals(k))
                clustering = "TRUE";
            else
                clustering = "FALSE";

            result = result + '\n' + tableName + "," + columnName + "," + columnType + "," + clustering + "," + "FALSE" + ","
                    + columnMin + "," + columnMax;

        }

        return result;


    }


    public static void checkDataTypes (String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, ParseException {

        //  #####   parsing a CSV file into vector of String[]  #####

        String min;
        String max;
        Vector<String[]> Data =new Vector<String[]>();
        String line = "";
        String splitBy = ",";
        int i =0;
        try
        {

            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] array= line.split(splitBy);
                Data.add(array);

            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        boolean tableFound = false;
        boolean colFound = false;
        for(int j =0 ;j<Data.size();j++){
            if(Data.get(j)[0].equals(tableName)) {
                min = Data.get(j)[Data.get(j).length - 2];
                tableFound=true;
                String colName = Data.get(j)[1];
                if (colNameValue.get(colName) != null) {
                    colFound= true;
                    if(colNameValue.get(colName) instanceof  Date){
                        Date d = (Date)colNameValue.get(colName);
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                        String strDate = dateFormat.format(d);
                        Date date1=new SimpleDateFormat("yyyy-mm-dd").parse(min);
                        Date date2 = new SimpleDateFormat("yyyy-mm-dd").parse(strDate);
                        //System.out.println(date1);
                        //System.out.println(date2);
                        if (date2.compareTo(date1) < 0)
                            throw new DBAppException("you entered date below minimum");
                        max = Data.get(j)[Data.get(j).length - 1];
                        Date datemax=new SimpleDateFormat("yyyy-mm-dd").parse(max);
                        if (date2.compareTo(datemax) > 0)
                            throw new DBAppException("you entered date above maximum");
                    }
                    else {
                        if (colNameValue.get(colName).toString().compareTo(min) < 0) {
                            throw new DBAppException("you entered value below minimum");
                        }
                        max = Data.get(j)[Data.get(j).length - 1];
                        if (colNameValue.get(colName).toString().compareTo(max) > 1) {
                            throw new DBAppException("you entered value above maximum");
                        }
                    }
                }
            }
        }

        //  ######   looping over table attributes   #######
        String columnName="";
        Set<String> columns  = colNameValue.keySet();

        for(String k : columns){
            columnName = k;
            String dataType =getType(colNameValue.get(k));

            try {
                if (dataType.equals( "NA"))
                    throw new DBAppException("invalid Data Type");
                String CSVType="";

                for (int j=0;j<Data.size();j++){
                    String[] attributes =Data.get(j);
                    if (attributes[0].equals(tableName)&& attributes[1].equals(columnName))
                        CSVType =attributes[2];
                }
                if (!CSVType.equals(dataType))
                    throw new DBAppException("invalid Data Type");



            }catch (Exception e){
                System.out.print(e.getMessage());
            }



        }


    }

    public static String getType (Object o){
        if (o instanceof String)
            return "java.lang.String";
        else if(o instanceof Integer)
            return "java.lang.Integer";
        else if (o instanceof Double)
            return "java.lang.Double";
        else  if (o instanceof Date)
            return "java.util.Date";
        else
            return "NA";
    }

    public static void serializePage(Page p){
        String pageName = p.PageID;
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+pageName+".bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(p);

            out.close();
            file.close();

            //   System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }

    }


    public static Page deserialize(String filePath){
        Page page = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            page = (Page) in.readObject();
            in.close();
            file.close();

            //      System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage()+filePath+"in deserialize page");
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage() +"in deserialize");
        }
        return page;
    }


    public int readConfig(String key) throws IOException {
        int nRows=0;
        FileReader reader=new FileReader("src/main/resources/DBApp.config");

        Properties p=new Properties();
        p.load(reader);
        return Integer.parseInt(p.getProperty(key));
    }
    public static void serializeTable(Table table){
        String tableName = table.tableName;
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+tableName+".bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(table);

            out.close();
            file.close();
            //   System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage()+ "in serialize table");
        }

    }
    /*public static Table deserializeTable(String tableName){
        Table table = null;
        String filePath = "src/main/resources/"+tableName+".bin";
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            table = (Table) in.readObject();
            in.close();
            file.close();

            System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }
        return table;
    }*/
    public Page getPage(String tableName , Hashtable<String,Object> colNameValue) throws DBAppException {
        Page p = null;
        //table doesnt exist
        Vector<String> tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
        if(!tableNames.contains(tableName))
            throw new DBAppException("table doesn't exist!");
        else {
            serializetableNames(tableNames);
            String filePath = "src/main/resources/data/"+tableName+".bin";
            Table t = (Table) DeserializeTable(filePath);
            String clustering = t.clusteringKey;
            //I have the primary key and can search with it
            if(colNameValue.get(clustering)!=null){
                int x =   Collections.binarySearch(t.min,colNameValue.get(clustering).toString());
                //the pk is the min at that page
                if(x>0){


                    p = deserialize("src/main/resources/data/"+tableName+x+".bin");
                }
                else{
                    int c = x-1;

                    p=deserialize("src/main/resources/data/"+tableName+c+".bin");

                }
            }
            else{
                //looping over all table pages
                for(int i =0 ; i<t.pagesPath.size();i++){
                    String pageID =t.tableName+i;
                    String filePath1 = "src/main/resources/data/"+pageID+".bin";
                    p = deserialize(filePath1);
                    //looping over each page
                    for(int j =0;j<p.list.size();j++){
                        String currRow = p.list.get(j).toString();
                        String entry = colNameValue.toString();
                        if(currRow.contains(entry)) {
                            return p;

                        }
                    }
                }
            }
        }

        return p;}
    public static int getRow(Page p, Hashtable<String, Object> colNameValue, String clusteringKey) throws DBAppException {
        int idx=0;
        //table doesnt exist
        //I have the primary key and can search with it
        if(colNameValue.get(clusteringKey)!=null){
            //getting the index by the clustering key
            idx = Collections.binarySearch(p.clusterings,colNameValue.get(clusteringKey).toString());
            if(idx>0)
                return idx;
            else if(p.overflowPage!=null &&p.overflowPage.list.size()!=0){
                int x = Collections.binarySearch(p.overflowPage.clusterings, colNameValue.get(clusteringKey).toString());
                if(x>0){
                    return x;
                }
                else
                    throw new DBAppException("The entered row doesn't exist");

            }
            else
                throw new DBAppException("The entered row doesn't exist");
        }
        else{

            for(int i =0; i<p.list.size();i++){
                String currRow = p.list.get(i).toString();
                String entry = colNameValue.toString();
                if(currRow.contains(entry))
                    return i;
            }
            if(p.overflowPage!=null &&p.overflowPage.list.size()!=0){
                for(int i =0; i<p.overflowPage.list.size();i++){
                    String currRow = p.overflowPage.list.get(i).toString();
                    String entry = colNameValue.toString();
                    if(currRow.contains(entry))
                        return i;
                }
            }

        }


        return idx;}



    public static Table DeserializeTable(String path){
        Table table = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            table = (Table) in.readObject();
            in.close();
            file.close();

            //  System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage()+"in deserialize table");
        }
        return table;


    }
    public  void serializeTableFunction(Table t){
        try
        {
            Vector<String> tableList = deserializeVectorS("src/main/resources/data/tablesList.bin");
            int vectorSize=tableList.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+t.tableName+vectorSize+".bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(t);

            out.close();
            file.close();

            //    System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage()+"in serialize table functin");
        }



    }
    public static void serializetableList(Vector<String> v ){
        try
        {
            int vectorSize=v.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/tablesList.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(v);

            out.close();
            file.close();

            //   System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage()+"serialize table list");
        }

    }

    public static Vector<String> deserializeVector(String filePath){
        Vector<String> v = new Vector<>();
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            v = (Vector<String>) in.readObject();
            in.close();
            file.close();

            //    System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage()+"deserialize vector");
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage()+"deserialize vectoe");
        }
        return v;


    }
    public static void serializeindicies(Vector<String> v ){
        try
        {
            int vectorSize=v.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/indicies.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(v);

            out.close();
            file.close();

            //  System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage()+"serialize table names");
        }

    }
    public static void serializetableNames(Vector<String> v ){
        try
        {
            int vectorSize=v.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/tableNames.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(v);

            out.close();
            file.close();

            //  System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage()+"serialize table names");
        }

    }
    public static Vector<String> deserializeVectorS(String filePath){
        Vector<String> v = new Vector<>();
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            v = (Vector<String>) in.readObject();
            in.close();
            file.close();

            //     System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage()+"'deserialize vectors'");
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage()+"'deserialize vectors'");
        }
        return v;


    }
    public void colNotFound (String tableName , Hashtable<String,Object> colNameValue) throws DBAppException {
        Vector<String[]> Data =new Vector<String[]>();
        String line = "";
        String splitBy = ",";
        try
        {

            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] array= line.split(splitBy);
                Data.add(array);

            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        HashSet<String> colNames = new HashSet<>();
        for(int i =0 ; i<Data.size();i++){
            if(Data.get(i)[0].equals(tableName)){
                colNames.add(Data.get(i)[1]);
            }
        }
        Set hash_set = colNameValue.keySet();

        if(!colNames.containsAll(hash_set)){
            throw new DBAppException("Columns entered not found in the table!");
        }
    }





    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static void  buildArray (int level, Vector array){
        if (level==1) {
            for(int i =0;i<10;i++){
                array.add("");
            }
            return;
        }
        else {

            int newLevel =--level;
            for (int i=0;i<10;i++)
                array.add(new Vector(10));
            // System.out.println("index's Grid"+array);
            for(int i =0;i<10;i++)
                buildArray(newLevel,(Vector) array.get(i));
        }

    }

    public static void updateMetadata (String[] attributes,String tableName) throws DBAppException {
        Vector<String[]> Data = new Vector<String[]>();
        String line = "";
        String splitBy = ",";
        int i = 0;
        try {

            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] array = line.split(splitBy);
                Data.add(array);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int k = 0; k < attributes.length; k++) {
            for (int j = 1; j < Data.size(); j++) {
                if (Data.get(j)[0].equals(tableName) && Data.get(j)[1].equals(attributes[k])) {
                    Data.get(j)[4]="TRUE";

                }

            }
        }
      String result ="";
        for (int m=1;m<Data.size();m++){
            for (int n =0;n<Data.get(m).length;n++){
                if(n==Data.get(m).length-1)
                    result+=Data.get(m)[n];
                else
                    result+=Data.get(m)[n]+",";

            }
            result+='\n';
        }
        String resultFinal = "Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max"+'\n'+result;
        try {
            FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv",false);
            csvWriter.write(resultFinal);
            csvWriter.flush();
            csvWriter.close();
        } catch(Exception e){
            System.out.print(e.getMessage());
        }




    }


    public static void createRanges (Index index){
        Vector<String[]> Data = new Vector<String[]>();
        String line = "";
        String splitBy = ",";
        int i = 0;
        try {

            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] array = line.split(splitBy);
                Data.add(array);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String min ="";
        String max ="";
        String type="";
        for (int k = 0; k < index.colNames.length; k++) {
            for (int j = 0; j < Data.size(); j++) {
                if (Data.get(j)[0].equals(index.tableName) && Data.get(j)[1].equals(index.colNames[k])) {
                    type=Data.get(j)[2];
                    min =Data.get(j)[5];
                    max=Data.get(j)[6];
                    createRangeList(index,index.colNames[k],type,min,max);

                }
            }
        }


    }


    public static void createRangeList (Index index ,String colName, String type, String min,String max){
        if (type.equals("java.lang.Integer")){
            int range = (int)Math.ceil(((Integer.parseInt(max)-Integer.parseInt(min))+1)/10.0);
            Vector r =  index.ranges.get(colName);
            r.add(min);
            for(int i=1;i<10;i++){
                int prevMin =Integer.parseInt(String.valueOf(r.get(i-1)));
                int currMin = prevMin+range;
                if(currMin>Integer.parseInt(max)){
                    r.add(max);
                }
                else
                    r.add(String.valueOf(currMin));
            }}
        else if(type.equals("java.lang.Double")){
            double range = Math.ceil(((Double.parseDouble(max)-Double.parseDouble(min))+1)/10.0);
            Vector r =  index.ranges.get(colName);
            r.add(min);
            for(int i=1;i<10;i++){
                double prevMin =Double.parseDouble((String.valueOf(r.get(i-1))));
                double currMin = prevMin+range;
                if(currMin>Double.parseDouble(max)){
                    r.add(max);
                }
                else
                    r.add(String.valueOf(currMin));
            }}

        else if (type.equals("java.lang.String")){
            if(!Character.isAlphabetic(max.charAt(0))){
                int range = (int)Math.ceil(((Integer.parseInt(max.replace("-",""))-Integer.parseInt(min.replace("-","")))+1)/10.0);
                Vector r =  index.ranges.get(colName);
                r.add(min);
                for(int i=1;i<10;i++){
                    int prevMin =Integer.parseInt(String.valueOf(r.get(i-1).toString().replace("-","")));
                    int currMin = prevMin+range;
                    if(currMin>Integer.parseInt(max.replace("-",""))){
                        r.add(max);
                    }
                    else
                        r.add(String.valueOf(currMin));
                }}
            else{
                int range = (int)Math.ceil(((max.toLowerCase(Locale.ROOT).charAt(0))-(min.toLowerCase(Locale.ROOT).charAt(0))+1)/10.0);
                Vector r =  index.ranges.get(colName);
                r.add((int)min.toLowerCase(Locale.ROOT).charAt(0));
                for(int i=1;i<10;i++){
                    int prevMin =Integer.parseInt(String.valueOf(r.get(i-1)));
                    int currMin = prevMin+range;
                    if(currMin>max.toLowerCase(Locale.ROOT).charAt(0))
                        r.add((int)max.toLowerCase(Locale.ROOT).charAt(0));
                    else
                        r.add(String.valueOf(currMin));
                }}}
        else  if (type.equals("java.util.Date")){
            String minDate = min.replace("-","");
            String maxDate = max.replace("-","");
            int range = (int)Math.ceil(((Integer.parseInt(maxDate)-Integer.parseInt(minDate))+1)/10.0);
            Vector r =  index.ranges.get(colName);
            r.add(minDate);
            for(int i=1;i<10;i++){
                int prevMin =Integer.parseInt(String.valueOf(r.get(i-1)));
                int currMin = prevMin+range;
                if(currMin>Integer.parseInt(maxDate))
                    r.add(String.valueOf(maxDate));
                r.add(String.valueOf(currMin));
            }
        }
    }
    public static void insertIntoBucketUpdate(String pagePath, Vector colValues , Index index,int currentDimension,Vector data, String dimensionValue ) {
        // System.out.println("I came update"+data.get(0).toString());

        String dimensionName = index.colNames[currentDimension];
        Vector currentRanges = index.ranges.get(dimensionName);
        Object currentValue = colValues.get(currentDimension);
        String bucketPath = null;
        int i = 0;//to get the range index
        //BASE CASE >>>  INSERTING INTO BUCKET

        if (currentDimension == index.colNames.length - 1) {
            Bucket bucket;
            for (i = 0; i < currentRanges.size(); i++) {
                //last bucket case
                if (i == currentRanges.size() - 1) {
                    bucketPath = (String) data.get(currentRanges.size() - 1);
                    dimensionValue += currentRanges.size() - 1;
                    break;
                } else {
                    String type = getType(dimensionName);
                    if (type.equals("java.lang.String")) {
                        Character c = currentValue.toString().charAt(0);
                        Character range = (Character) currentRanges.get(i);
                        if (c <= range) {
                            bucketPath = (String) data.get(i);
                            dimensionValue += i;
                            break;
                        }
                    } else if (type.equals("java.lang.Integer")) {
                        if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                            bucketPath = (String) data.get(i);
                            dimensionValue += i;
                            break;
                        }
                    } else if (type.equals("java.lang.Double")) {
                        if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                            bucketPath = (String) data.get(i);
                            dimensionValue += i;
                            break;
                        }
                    } else if (type.equals("java.util.Date")) {
                        String currDate = ((Date) currentValue).toString();
                        int numcurDate = Integer.parseInt(currDate);
                        int rangeDate = Integer.parseInt(currentRanges.get(i).toString());
                        if (numcurDate <= rangeDate) {
                            bucketPath = (String) data.get(i);
                            dimensionValue += i;
                            break;
                        }
                    } else {
                        try {
                            throw new DBAppException("WRONG DATATYPE");
                        } catch (DBAppException e) {
                            System.out.println(e.getMessage());
                        }
                    }


                }
            }




            //%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%   Dangerous Area   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%//



            ///here curly prases of the method
            String s = index.indexId;
            //case no created bucket was found in the grid
            bucket = null;
            if (bucketPath == null) {
                Bucket newBucket = new Bucket(index.indexId + dimensionValue, "src/main/resources/data/"+index.indexId+".bin");
                bucketPath = "src/main/resources/data/" + newBucket.BucketId + ".bin";
                data.insertElementAt(bucketPath,i);

            }
            //String bucketPath ="src/main/resources/data/"+index.indexId+dimensionValue+".bin";

            else {
                bucket = Bucket.DeserializeBucket(bucketPath);
                if (contains(index.colNames, index.clusteringKey)) {
                    int m = 0;
                    for (int j = 0; j < index.colNames.length; j++) {
                        if (index.colNames[j].equals(index.clusteringKey)) {
                            m = j;
                        }
                    }

                    Vector a = bucket.list.get(currentValue);

                    if (a == null) {
                        if (bucket.noOfEntries < bucket.max) {
                            bucket.noOfEntries++;
                            a = new Vector<String>();
                        }
                        //adding to the overflow
                        else {
                            a = new Vector<String>();
                            a.add(pagePath);
                            if (bucket.overFlow == null) {
                                bucket.overFlow = new Bucket(bucket.BucketId + "Over", "src/main/resources/data/"+index.indexId+".bin");
                                bucket.overFlow.list.put((String) colValues.get(m), a);
                            } else {
                                a.add(pagePath);
                                bucket.overFlow.list.put((String) colValues.get(m), a);
                            }
                            bucket.overFlow.serializeBucket();
                        }
                    }
                    else{
                        a.add(pagePath);
                        bucket.list.put((String) colValues.get(m), a);
                    }
                }
                //use left most col if primary key doesn't exist
                else {
                    Vector a = bucket.list.get(colValues.get(0));
                    if (a == null) {
                        if (bucket.noOfEntries < bucket.max) {
                            a = new Vector<String>();
                            bucket.noOfEntries++;
                        } else {
                            a = new Vector<String>();
                            a.add(pagePath);
                            if (bucket.overFlow == null) {
                                bucket.overFlow = new Bucket(bucket.BucketId + "Over", "src/main/resources/data/"+index.indexId+".bin");
                                bucket.overFlow.list.put((String) colValues.get(0), a);
                            } else {
                                a.add(pagePath);
                                bucket.overFlow.list.put((String) colValues.get(0), a);
                            }
                        }

                    }
                    a.add(pagePath);
                    bucket.list.put((String) colValues.get(0), a);
                }
            }
            bucket.serializeBucket();
        }
        // public static void insertIntoBucketUpdate(String pagePath, Vector colValues , Index index ,int currentDimension ,Vector data, String dimensionValue )
        else{
            int k=-1;

            for (i = 0; i < currentRanges.size(); i++) {
                if (i == currentRanges.size() - 1) {
                    k=i;
                    dimensionValue += currentRanges.size() - 1;
                    break;
                } else {
                    String type = getType(dimensionName);
                    if (type.equals("java.lang.String")) {
                        Character c = currentValue.toString().charAt(0);
                        Character range = (Character) currentRanges.get(i);
                        if (c <= range) {
                            k=i;
                            dimensionValue += i;
                            break;
                        }
                    } else if (type.equals("java.lang.Integer")) {
                        if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                            k=i;
                            dimensionValue += i;
                            break;
                        }
                    } else if (type.equals("java.lang.Double")) {
                        if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                            k=i;
                            dimensionValue += i;
                            break;
                        }
                    } else if (type.equals("java.util.Date")) {
                        String currDate = ((Date) currentValue).toString();
                        int numcurDate = Integer.parseInt(currDate);
                        int rangeDate = Integer.parseInt(currentRanges.get(i).toString());
                        if (numcurDate <= rangeDate) {
                            k=i;
                            dimensionValue += i;
                            break;
                        }
                    } else {
                        try {
                            throw new DBAppException("WRONG DATATYPE");
                        } catch (DBAppException e) {
                            System.out.println(e.getMessage());
                        }
                    }


                }
            }
            insertIntoBucketUpdate(pagePath,colValues,index,currentDimension+1,(Vector) data.get(k),dimensionValue);

        }
    }


    /* public void createIndex(String tableName, String[] columnNames) throws DBAppException {
         Index index = new Index(tableName,columnNames);

         loopPages(tableName,columnNames,index);

     }*/
    public static void updateIndex(String pagePath , Hashtable<String,Object>colnamevalue , String tableName){
        Table target = DeserializeTable("src/main/resources/data/"+tableName+".bin");
        for(int i =0 ;i<target.indicies.size();i++ ){
            Index index = Index.DeserializeIndex(target.indicies.get(i));
            Vector colValues = new Vector<String>();
            for(int j  =0 ; j<index.colNames.length;j++){
                colValues.add(colnamevalue.get(index.colNames[i]));
                insertIntoBucketUpdate (pagePath,colValues , index,0,index.grid,"");
            }
        }

    }
    public static void insertIntoBucket (String pagePath, int[] indexes , Index index,int currentDimension,Vector data , Hashtable<String , Object> colnameval ) {
        // System.out.println("data array"+data);
        String bucketpath = null;
        Bucket bucket =null;
        String dimensionValue="";
       /* for(int i =0;i<indexes.length;i++){
            System.out.println("indexes"+i+": " +indexes[i]);
        }*/
        if(currentDimension== index.colNames.length-1){
            //////////////// what the hell is this ????????
            for(int i=0;i<indexes.length;i++)
                dimensionValue+=indexes[i];
            bucketpath = (String) data.get(indexes[currentDimension]);
            //never comes to this case !!
            if(bucketpath == "") {
                //  System.out.println("I came to bucket ==null");
                ///indexId + current dimension ??
                // System.out.println("DATA ARRAY SIZE: " + data.size());
                bucket = new Bucket(index.indexId + dimensionValue, "src/main/resources/data/"+index.indexId+".bin");
                if(colnameval.containsKey(index.clusteringKey)){
                    Vector v = new Vector<String>();
                    v.add(pagePath);
                    if(colnameval.get(index.clusteringKey) instanceof Date){
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                        String strDate = dateFormat.format(colnameval.get(index.clusteringKey));
                        String numcurDate = strDate.replace("-","");
                        bucket.list.put(numcurDate,v);
                    }
                    else
                        bucket.list.put(index.clusteringKey,v);

                }
                else{
                    Vector v = new Vector<String>();
                    v.add(pagePath);
                    if(colnameval.get(index.clusteringKey) instanceof Date){
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                        String strDate = dateFormat.format(colnameval.get(index.clusteringKey));
                        String numcurDate = strDate.replace("-","");
                        bucket.list.put(numcurDate,v);
                    }
                    else
                        bucket.list.put(index.colNames[0],v);
                }
                // System.out.println(data);

                data.insertElementAt("src/main/resources/data/" + bucket.BucketId + ".bin",indexes[indexes.length-1]);

            }
            else
                bucket = Bucket.DeserializeBucket(bucketpath);
            if(contains(index.colNames,index.clusteringKey)){
                Vector a = bucket.list.get(colnameval.get(index.clusteringKey));
                if(a== null){
                    if(bucket.noOfEntries<bucket.max) {
                        bucket.noOfEntries++;
                        a = new Vector<String>();
                    }
                    else{
                        a= new Vector<String>();
                        a.add(pagePath);
                        if(bucket.overFlow==null) {
                            bucket.overFlow = new Bucket(bucket.BucketId + "Over", "src/main/resources/data/"+index.indexId+".bin");
                            if(colnameval.get(index.clusteringKey) instanceof Date){
                                DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                                String strDate = dateFormat.format(colnameval.get(index.clusteringKey));
                                String numcurDate = strDate.replace("-","");
                                bucket.list.put(numcurDate,a);
                            }
                            else
                                bucket.overFlow.list.put((String) colnameval.get(index.clusteringKey), a);
                        }
                        else{
                            a.add(pagePath);
                            if(colnameval.get(index.clusteringKey) instanceof Date){
                                DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                                String strDate = dateFormat.format(colnameval.get(index.clusteringKey));
                                String numcurDate = strDate.replace("-","");
                                bucket.list.put(numcurDate,a);
                            }
                            else
                                bucket.list.put(colnameval.get(index.clusteringKey).toString(), a);
                        }
                    }

                }
                a.add(pagePath);
                bucket.list.put(colnameval.get(index.clusteringKey).toString(), a);

            }
            //use left most col if primary key doesn't exist
            else{
                Vector a = bucket.list.get(colnameval.get(0));
                if(a== null){
                    if(bucket.noOfEntries<bucket.max) {
                        a = new Vector<String>();
                        bucket.noOfEntries++;
                    }
                    else{
                        a= new Vector<String>();
                        a.add(pagePath);
                        if(bucket.overFlow==null) {
                            bucket.overFlow = new Bucket(bucket.BucketId + "Over", "src/main/resources/data/"+index.indexId+".bin");
                            if(colnameval.get(index.clusteringKey) instanceof Date){
                                DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                                String strDate = dateFormat.format(colnameval.get(index.clusteringKey));
                                String numcurDate = strDate.replace("-","");
                                bucket.list.put(numcurDate,a);
                            }
                            else
                                bucket.overFlow.list.put((String) colnameval.get(0), a);
                        }
                        else{
                            a.add(pagePath);
                            if(colnameval.get(index.clusteringKey) instanceof Date){
                                DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                                String strDate = dateFormat.format(colnameval.get(index.clusteringKey));
                                String numcurDate = strDate.replace("-","");
                                bucket.list.put(numcurDate,a);
                            }
                            else
                                bucket.list.put((String) colnameval.get(0), a);
                        }
                    }

                }
                a.add(pagePath);
                if(colnameval.get(index.clusteringKey) instanceof Date){
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                    String strDate = dateFormat.format(colnameval.get(index.clusteringKey));
                    String numcurDate = strDate.replace("-","");
                    bucket.list.put(numcurDate,a);
                }
                else
                    bucket.list.put((String) colnameval.get(0), a);
            }


        }
        else{
            //  System.out.println("indexes dimension"+currentDimension);
            currentDimension++;
            // System.out.println(currentDimension);
            insertIntoBucket ( pagePath,indexes ,index,currentDimension, (Vector) data.get(indexes[currentDimension]), colnameval );

        }
        //System.out.println("bucketPath is"+bucketpath);
        if(currentDimension==index.colNames.length-1&& bucket!=null){
            bucket.serializeBucket();
            index.serializeIndex();
        }
    }
    public static boolean contains (String [] arr, String s ){
        for(int i =0 ;i<arr.length;i++){
            if(arr[i].equals(s))
                return true;
        }
        return false;
    }
    public static void loopPages(String tableName, String[] columnNames,Index index){
        //System.out.println("I came loop method");
        Table table =DeserializeTable("src/main/resources/data/" + tableName + ".bin");
        // System.out.println("I entered the loop");

        for(int i=0;i<table.pagesPath.size();i++){
            loopPage(tableName, table.pagesPath.get(i), columnNames, index);
            Page page=deserialize(table.pagesPath.get(i));
            Page overflow=new Page("name");
            if (page.overflowPage != null) {

                overflow = deserialize("src/main/resources/data" + "/" + tableName + i + "Over" + ".bin");
                String p="src/main/resources/data" + "/" + tableName + i + "Over" + ".bin";  // overflow page path
                loopPage(tableName, p, columnNames, index);
            }
            serializePage(overflow);
            serializePage(page);
        }
        serializeTable(table);
    }
    public static void loopPage(String tableName, String filepath, String[] columnNames,Index index)  {
        Page page=deserialize(filepath);

        for(int j=0;j<page.clusterings.size();j++){
            // Hashtable<String,Integer> indexes=new Hashtable<String,Integer>();
            int [] indexes =new int [columnNames.length];
            Hashtable row=page.list.get(j);
            for(int i=0;i<columnNames.length;i++){
                Object value =row.get(columnNames[i]);
                Vector ranges=index.ranges.get(columnNames[i]);// the vector of MIN values  of columnNames[i]
                for(int g=0;g<ranges.size();g++){       // to know which index of the cell we should insert in
                    String type=getType(value);
                    //System.out.println(value);
                    if(type.equals("java.lang.String")){
                        if(!Character.isAlphabetic(value.toString().charAt(0))){
                            int numericRange = Integer.parseInt(ranges.get(g).toString().replace("-",""));
                            int numericValue = Integer.parseInt(value.toString().replace("-",""));
                            if(numericValue<=numericRange){
                                if(g==0)
                                    indexes[i]=0;
                                else
                                    indexes[i]=g-1;
                                break;
                            }
                        }
                        else{
                            int c = Character.getNumericValue(value.toString().charAt(0));
                            System.out.println(ranges.get(g));
                            int range =  Integer.parseInt(ranges.get(g).toString());
                            if (c<=range) {
                                if(g==0)
                                    indexes[i]=0;
                                else
                                    indexes[i]=g-1;
                                break;
                            }}
                    }
                    else if(type.equals("java.lang.Integer")){
                        if (Integer.parseInt(value.toString())<(Integer.parseInt(ranges.get(g).toString()))) {
                            if(g==0)
                                indexes[i]=0;
                            else
                                indexes[i]=g-1;
                            break;
                        }
                    }
                    else if(type.equals("java.lang.Double")){
                        if ((Double.parseDouble(value.toString()) <(Double.parseDouble(ranges.get(g).toString())))) {
                            if(g==0)
                                indexes[i]=0;
                            else
                                indexes[i]=g-1;
                            break;
                        }
                    }
                    else if(type.equals("java.util.Date")){
                        Date currDate = (Date) value;
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");

                        String strDate = dateFormat.format(currDate);

                        int numcurDate = Integer.parseInt(strDate.replace("-",""));
                        int rangeDate = Integer.parseInt(ranges.get(g).toString());
                        if (numcurDate<=rangeDate) {
                            //System.out.println("num"+numcurDate);
                            //System.out.println("range"+rangeDate+" "+g);

                            if(g==0)
                                indexes[i]=0;
                            else
                                indexes[i]=g-1;
                            break;
                        }
                    }
                    else
                    {
                        try {
                            throw new DBAppException("WRONG DATATYPE");
                        } catch (DBAppException e) {
                            System.out.println(e.getMessage());
                        }
                    }



                }

            }
            //System.out.println(index.ranges);
            //System.out.println(index.clusteringKey);
            //System.out.println(index.colNames);

            //System.out.println("indexes array"+indexes[0]);
            //System.out.println("htbl"+row);

            //public static void insertIntoBucket (String pagePath, int[] indexes , Index index,int currentDimension,Vector data , Hashtable<String , Object> colnameval ) {
            insertIntoBucket(filepath,indexes,index,0,index.grid,row);
            // KareemMethod(j,filepath,indexes,index);// j is the index of the row inside the page
            //filepath is the path of the page containing the row
            //indexes is the array  that contains the tuple that has the indexes
            // that the row should be inserted in
            //index is the Index you should insert the path in

        }

        serializePage(page);
    }



    public static String getIndex (String tableName, Hashtable<String,Object> columns){
        Table table= DeserializeTable("src/main/resources/data/" + tableName + ".bin");
        Vector<String> colNames=new Vector<String>();
        for (Map.Entry<String, Object> e : columns.entrySet()){
            colNames.add(e.getKey());
        }
        int max=0;
        String indexPath ="";
        int counter =0;
        for (int i =0;i<table.indicies.size();i++){
            String[] tokens =table.indicies.get(i).split("_");
            counter=0;
            for(int j=2; j<tokens.length;j++){
                String key = tokens[j];
                if(j==tokens.length-1)
                    key=tokens[j].substring(0,tokens[j].length()-5);
                for(int k=0;k<colNames.size();k++){
                    if(key.equals(colNames.get(k))){
                        counter++;

                    }

                }

            }
            if(counter>= max){
                max = counter ;
                indexPath=table.indicies.get(i);
            }

        }
        if (max==0)
            return null;
        else
            return indexPath;

    }

    public static boolean containsKey (String[] array,String key){
        for(int i=0;i< array.length;i++)
            if (array[i].equals(key))
                return true ;
        return false;
    }

    public static void deleteUsingIndex(Index index,Hashtable<String,Object>  existingCol,Hashtable<String,Object> nonExistingCol,int currentDimension,Vector data , Hashtable<String,Object>colNameValue) throws DBAppException {
        String dimensionName = index.colNames[currentDimension];
        Vector currentRanges = index.ranges.get(dimensionName);
        Object currentValue = existingCol.get(dimensionName);
        String bucketPath = null;
        int i = 0;//to get the range index
        // check if it is the last dimension or not
        if (index.colNames.length - 1 == currentDimension) {

            if (currentValue == null) {
                for (int j = 0; j < 10; j++) {
                    bucketPath = (String) data.get(j);
                    if (bucketPath == null)
                        return;
                    else
                        deleteFromBucket(bucketPath,colNameValue);

                }

            } else {
                for (i = 0; i < currentRanges.size(); i++) {
                    if (i == currentRanges.size() - 1) {
                        bucketPath = (String) data.get(i);
                        if (bucketPath == null)
                            return;
                        else
                            deleteFromBucket(bucketPath, colNameValue);
                    } else {
                        String type = getType(dimensionName);
                        if (type.equals("java.lang.String")) {
                            Character c = currentValue.toString().charAt(0);
                            Character range = (Character) currentRanges.get(i);
                            if (c <= range) {
                                bucketPath = (String) data.get(i);
                                deleteFromBucket(bucketPath, colNameValue);
                                break;
                            }
                        } else if (type.equals("java.lang.Integer")) {
                            if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                                bucketPath = (String) data.get(i);
                                deleteFromBucket(bucketPath, colNameValue);
                                break;
                            }
                        } else if (type.equals("java.lang.Double")) {
                            if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                                bucketPath = (String) data.get(i);
                                deleteFromBucket(bucketPath, colNameValue);
                                break;
                            }
                        } else if (type.equals("java.util.Date")) {
                            String currDate = ((Date) currentValue).toString();
                            int numcurDate = Integer.parseInt(currDate);
                            int rangeDate = Integer.parseInt(currentRanges.get(i).toString());
                            if (numcurDate <= rangeDate) {
                                bucketPath = (String) data.get(i);
                                deleteFromBucket(bucketPath, colNameValue);
                                break;
                            }
                        } else {
                            try {
                                throw new DBAppException("WRONG DATATYPE");
                            } catch (DBAppException e) {
                                System.out.println(e.getMessage());
                            }
                        }


                    }


                }
            }
        }
        else{
            int k=-1;

            for (i = 0; i < currentRanges.size(); i++) {
                if (i == currentRanges.size() - 1) {
                    k=i;
                    break;
                } else {
                    String type = getType(dimensionName);
                    if (type.equals("java.lang.String")) {
                        Character c = currentValue.toString().charAt(0);
                        Character range = (Character) currentRanges.get(i);
                        if (c <= range) {
                            k=i;
                            break;
                        }
                    } else if (type.equals("java.lang.Integer")) {
                        if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                            k=i;
                            break;
                        }
                    } else if (type.equals("java.lang.Double")) {
                        if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                            k=i;
                            break;
                        }
                    } else if (type.equals("java.util.Date")) {
                        String currDate = ((Date) currentValue).toString();
                        int numcurDate = Integer.parseInt(currDate);
                        int rangeDate = Integer.parseInt(currentRanges.get(i).toString());
                        if (numcurDate <= rangeDate) {
                            k=i;
                            break;
                        }
                    } else {
                        try {
                            throw new DBAppException("WRONG DATATYPE");
                        } catch (DBAppException e) {
                            System.out.println(e.getMessage());
                        }
                    }


                }
            }
            deleteUsingIndex(index,existingCol,nonExistingCol,currentDimension+1,(Vector) data.get(k),colNameValue);








        }


    }


    public static void  deleteFromBucket(String bucketPath,Hashtable<String,Object> columnNameValue) throws DBAppException {
        Bucket bucket=Bucket.DeserializeBucket(bucketPath);
        String indexingcol = null;
        String indexPath = bucket.indexPath;
        Index i =Index.DeserializeIndex(indexPath);
        String key = i.clusteringKey;
        String tableName = i.tableName;
        int row =-1;
        if(columnNameValue.containsKey(key)){
            indexingcol=key;
        }
        else
            indexingcol = i.colNames[0];
        Vector<String> pagesList = bucket.list.get(indexingcol);

        if(!columnNameValue.containsKey(indexingcol)){
            pagesList = new Vector<>();
            for(int j =0;j<bucket.list.size();j++){
                for(int k =0;k<bucket.list.get(j).size();k++){
                    if(!pagesList.contains(bucket.list.get(j).get(k)))
                        pagesList.add(bucket.list.get(j).get(k));
                }
            }
        }
        Vector<String> deletedPages = new Vector<>();
        for(int j=0;j<pagesList.size();j++){
            Page p = deserialize(pagesList.get(j));
            row = getRow(p,columnNameValue,key);
            if(row!=-1){
                p.list.removeElementAt(row);
                deletedPages.add(pagesList.get(j));
            }
            serializePage(p);
        }
        for(int j=0;j<deletedPages.size();j++){
            if(pagesList.contains(deletedPages.get(j))){
                pagesList.remove(deletedPages.get(j));
            }
        }
        i.serializeIndex();
        bucket.serializeBucket();
    }
    public Iterator selectFromTable (SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        Table table = DeserializeTable("src/resources/data/"+sqlTerms[0]._strTableName+".bin");
        if(table!=null){
            try{
                String tableName = sqlTerms[0]._strTableName; // table name I'm doing conditions on
                String[] columns = new String[sqlTerms.length]; // columns that is included in the condition array(sqlTerms)
                int noOfConditions = sqlTerms.length;
                //getting the column names that are in the sqlterms array
                for (int i = 0; i<sqlTerms.length; i++){
                    columns[i] = sqlTerms[i]._strColumnName;
                }
                //reading the metadata file to check if the table has an index or not
                String line = "";
                String splitBy = ",";
                int i =0;
                Vector<String[]> Data =new Vector<String[]>();
                try{
                    BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                    while ((line = br.readLine()) != null)   //returns a Boolean value
                    {
                        String[] array= line.split(splitBy);
                        Data.add(array);

                    }
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
                // end of reading


                Vector<String> indexedColumns = new Vector<String>();
                Vector<String> nonIndexedColumns = new Vector<String>();

                //checking which columns are indexed and which are not (from the metadata)
                Vector<SQLTerm> conditionsOfNonIndexedColumns=new Vector<SQLTerm>();
                conditionsOfIndexedColumns=new Vector<SQLTerm>();

                for (int j = 0; j<columns.length; j++){
                    for (int k = 0; k<Data.size();k++){
                        if ((Data.get(k)[0]).equals(tableName)){
                            if (Data.get(k)[1].equals(columns[j])){
                                if (Data.get(k)[4].equals("TRUE")) {
                                    indexedColumns.add(columns[j]);
                                    for (int h = 0; h < sqlTerms.length; h++) {
                                        if (sqlTerms[h]._strColumnName.equals(columns[j])) {
                                            conditionsOfIndexedColumns.add(sqlTerms[h]);
                                            break;
                                        }
                                    }
                                }
                                else
                                    nonIndexedColumns.add(columns[j]);
                                for(int h=0;h<sqlTerms.length;h++){
                                    if(sqlTerms[h]._strColumnName.equals(columns[j])){
                                        conditionsOfNonIndexedColumns.add(sqlTerms[h]);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                // end of checking


                Hashtable<SQLTerm, Vector<Hashtable<String, Object>>> rowsOfIndexedConditions = new Hashtable<SQLTerm, Vector<Hashtable<String,Object>>>();
                //Which Index Should I Use?
                Hashtable<String, Vector<String>> appropriateIndexColumns= returnAppropriateIndex(tableName, indexedColumns);
                Vector<String> unknown = appropriateIndexColumns.get("Unknown");
                appropriateIndexColumns.remove("Unknown");
                if(unknown.size()!=0){
                    nonIndexedColumns.addAll(unknown);
                }
                Set<String> indexids  = appropriateIndexColumns.keySet();
                String id;
                for(String k : indexids) {
                    id = k;
                    Index index = Index.DeserializeIndex("src/resources/data/" + id + ".bin");
                    if (index != null) {
                        SQLTerm[] arrayToBeInsertedIntoRecursiveMethod = new SQLTerm[conditionsOfIndexedColumns.size()];
                        String[] colsOfIndex = index.colNames;
                        for (int n = 0; n < index.colNames.length; n++) {
                            SQLTerm temp = new SQLTerm();
                            for (int n1 = 0; n1 < conditionsOfIndexedColumns.size(); n1++) {
                                if (conditionsOfIndexedColumns.get(n1)._strColumnName.equals(colsOfIndex[n])) {
                                    temp = conditionsOfIndexedColumns.get(n1);
                                    break;
                                }
                            }
                            arrayToBeInsertedIntoRecursiveMethod[n] = temp;
                        }
                        Vector<String> vectorToBeInsertedIntoRecursiveMethod = new Vector<String>();
                        Vector<String> bucketsPaths = recursiveMethod(arrayToBeInsertedIntoRecursiveMethod, index, index.grid, vectorToBeInsertedIntoRecursiveMethod);
                        rowsOfIndexedConditions = loopBuckets(bucketsPaths, arrayToBeInsertedIntoRecursiveMethod);
                        // unknown function
                        String[] indexedCols;
                        Hashtable<String, String[]> IndexColsForOneIndex = new Hashtable<String, String[]>();
                        //  table = DeserializeTable("src/main/resources/data/" + tableName + ".bin");
                        Vector<String> indexes = table.indicies;
                        for (int i2 = 0; i2 < indexes.size(); i++) {
                            Index index1 = Index.DeserializeIndex(indexes.get(i2));
                            indexedCols = intersection(index1.colNames, columns);
                            IndexColsForOneIndex.put(index1.indexId, indexedCols);
                        }
                        //end

                        // condition on columns have index
                        for (int i1 = 0; i1 < noOfConditions; i1++) {
                            if (indexedColumns.contains(sqlTerms[i1]._strColumnName)) {
                                //searchInIndex(sqlTerms[i1]);
                            }
                        }
                    }
                    //end
                    SQLTerm[] conditions = new SQLTerm[conditionsOfNonIndexedColumns.size()];

                    Hashtable<SQLTerm, Vector<Hashtable<String, Object>>> rows_resulted_from_nonindexed_columns = loopPagesWithCondition(tableName, conditionsOfNonIndexedColumns.toArray(conditions));

                    Hashtable<SQLTerm, Vector<Hashtable<String, Object>>> hashtableOfAllConditions = new Hashtable<SQLTerm, Vector<Hashtable<String, Object>>>();
                    hashtableOfAllConditions.putAll(rows_resulted_from_nonindexed_columns);
                    hashtableOfAllConditions.putAll(rowsOfIndexedConditions);

                    Hashtable<SQLTerm, Vector<Hashtable<String, Object>>> result = new Hashtable<SQLTerm, Vector<Hashtable<String, Object>>>();
                    for (int m = 0; m < sqlTerms.length; m++) {
                        result.put(sqlTerms[m], hashtableOfAllConditions.get(sqlTerms[i]));
                    }

                    Vector<Hashtable<String, Object>> finalResult = loopOnOperators(result, arrayOperators);
                    Iterator finalFinalResult = finalResult.iterator();
                    index.serializeIndex();
                    return finalFinalResult;
                }}
            catch(Exception e){
                System.out.print(e.getMessage());
            }}
        return null;
    }

   /* public Hashtable<String, Vector<String>> returnAppropriateIndex(String tableName, Vector<String> indexedColumns){
       // Table table = DeserializeTable("src/main/resources/data/" + tableName + ".bin");
        Hashtable<String, Vector<String>> res = columnsOfEachIndex(tableName, indexedColumns);
       // Vector<String> indexes = table.indicies;
       /* for (int k = 0; k<indexes.size(); k++){
            Index index = Index.DeserializeIndex(indexes.get(k));
            if(indexedColumns.contains(index.colNames)){
                serializeTable(table);
                index.serializeIndex();
                ;

            }
        }
       // serializeTable(table);
        return null;
    }*/

    public Hashtable<String, Vector<String>> returnAppropriateIndex(String tableName, Vector<String> indexedColumns) {
        Hashtable<String, Vector<String>> result = new Hashtable<String, Vector<String>>();
        Vector<String> unknown = new Vector<String>();
        Table table = DeserializeTable("src/resources/data/" + tableName + ".bin");
        try{
            Vector<String> indexes = table.indicies;
            for (int i = 0; i < indexes.size(); i++) {
                Index index = Index.DeserializeIndex(indexes.get(i));
                if (index.colNames.equals((String[]) indexedColumns.toArray())) {
                    result.clear();
                    result.put(index.indexId, indexedColumns);
                    return result;
                }
                String[] arr = intersection((String[]) Arrays.stream(index.colNames).toArray(), (String[]) indexedColumns.stream().toArray());
                if (arr.equals(index.colNames)){
                    Vector<String> res = new Vector<String>();
                    res.addAll(Arrays.asList(arr));
                    result.put(index.indexId,res);
                    indexedColumns.removeAll(Arrays.asList(arr));
                }
            }
            unknown.addAll(indexedColumns);
            result.put("Unknown",unknown);}
        catch(Exception e){
            System.out.println(e.getMessage());
        }
        return result;
    }
    public Vector<Hashtable<String,Object>> loopOnOperators(Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> cons, String[] strarrOperators){
        Set<SQLTerm> keys = cons.keySet();
        SQLTerm[] keysArray = keys.toArray(new SQLTerm[keys.size()]);
        Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();
        for (int i = 0; i<strarrOperators.length; i++){
            if(i==0){
                result = operatorMethod(cons.get(keysArray[i]),cons.get(keysArray[i+1]),strarrOperators[i]);
            }
            else{
                result = operatorMethod(result, cons.get(keysArray[i+1]),strarrOperators[i]);
            }
        }
        return result;
    }
    ///////////////////////
    public Vector<Hashtable<String,Object>> operatorMethod(Vector<Hashtable<String,Object>> v1, Vector<Hashtable<String,Object>> v2, String operator){
        Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();
        switch (operator){
            case "OR": result = OR(v1,v2); break;

            case "AND": result = AND(v1,v2); break;

            case "XOR": result = XOR(v1,v2); break;

            default: break;
        }

        return result;
    }


    public Vector<Hashtable<String,Object>> OR(Vector<Hashtable<String,Object>> v1, Vector<Hashtable<String,Object>> v2){
        Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();
        HashSet<Hashtable<String,Object>> set = new HashSet<Hashtable<String,Object>>();
        set.addAll(v1);
        set.addAll(v2);
        result.addAll(set);
        return result;
    }

    public Vector<Hashtable<String,Object>> AND(Vector<Hashtable<String,Object>> v1, Vector<Hashtable<String,Object>> v2){
        Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();
        HashSet<Hashtable<String,Object>> set = new HashSet<Hashtable<String,Object>>();
        set.addAll(v1);
        set.retainAll(v2);
        result.addAll(set);
        return result;
    }


    public Vector<Hashtable<String, Object>> XOR(Vector<Hashtable<String, Object>> v1, Vector<Hashtable<String, Object>> v2){
        Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();
        HashSet<Hashtable<String,Object>> set = new HashSet<Hashtable<String,Object>>();
        set.addAll(v1);
        set.addAll(v2);
        v1.retainAll(v2); //a now has the intersection of a and b
        set.removeAll(v1);
        result.addAll(set);
        return result;
    }

    ////////////////////////////////////

    public String[] intersection(String[] s1, String[] s2){

        HashSet<String> set = new HashSet<>();

        set.addAll(Arrays.asList(s1));

        set.retainAll(Arrays.asList(s2));

        System.out.println(set);

        //convert to array
        String[] intersection = {};
        return set.toArray(intersection);

    }
    public Vector<String> recursiveMethod(SQLTerm[] conditions, Index index, Vector grid, Vector<String> bucketsPaths) throws DBAppException//assuming conditions are sorted
    {
        if (conditions.length==1){
            SQLTerm currentCondition = conditions[0];
            String colName = currentCondition._strColumnName;
            Vector ranges = index.ranges.get(colName);
            Vector<Integer> indexes = new Vector<Integer>();
            int indexInGrid = recursiveMethodHelper(ranges, currentCondition._objValue);
            Vector<Integer> indexVectorInGrid = subIndexes(currentCondition,ranges,indexInGrid);
            SQLTerm[] conditions1 = new SQLTerm[conditions.length-1];
            for (int i = 0; i<indexVectorInGrid.size(); i++)
            {
                bucketsPaths.add((String)grid.get(indexVectorInGrid.get(i)));
            }
        }
        else{
            SQLTerm currentCondition = conditions[0];
            String colName = currentCondition._strColumnName;
            Vector ranges = index.ranges.get(colName);
            Vector<Integer> indexes = new Vector<Integer>();
            int indexInGrid = recursiveMethodHelper(ranges, currentCondition._objValue);
            Vector<Integer> indexVectorInGrid = subIndexes(currentCondition,ranges,indexInGrid);
            SQLTerm[] conditions1 = new SQLTerm[conditions.length-1];
            for(int j = 1; j<conditions.length; j++){
                conditions1[j-1]=conditions[j];
            }
            for (int i = 0; i<indexVectorInGrid.size(); i++){
                recursiveMethod(conditions1,index,(Vector)grid.get(indexVectorInGrid.get(i)),bucketsPaths);
            }
        }
        //[[1,2],[3,4]]
        return bucketsPaths;
    }

    public int recursiveMethodHelper(Vector ranges, Object value){
        for (int g = 0; g < ranges.size(); g++) {       // to know which index of the cell we should insert in
            String type = getType(value);

            if (type.equals("java.lang.String")) {
                if (((String) value).compareTo((String) ranges.get(g)) < 0)
                    return g-1;
            } else if (type.equals("java.lang.Integer")) {
                if (((Integer) value).compareTo((Integer) ranges.get(g)) < 0)
                    return g-1;
            } else if (type.equals("java.lang.Double")) {
                if (((Double) value).compareTo((Double) ranges.get(g)) < 0)
                    return g-1;
            } else if (type.equals("java.util.Date")) {
                if (((Date) value).compareTo((Date) ranges.get(g)) < 0)
                    return g-1;
            } else {
                try {
                    throw new DBAppException("WRONG DATATYPE");
                } catch (DBAppException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
        return -1;
    }
    public Vector<Integer> subIndexes(SQLTerm condition, Vector ranges, int currentPosition) throws DBAppException { //i for conditions vector, g for ranges
        String operator = condition._strOperator;
        Vector<Integer> result = new Vector<Integer>();
        switch (condition._strOperator) {
            case "<": {
                for (int i = 0; i<currentPosition; i++){
                    result.add(i);
                }
                break;
            }
            case "<=": {
                for (int i = 0; i<=currentPosition; i++){
                    result.add(i);
                }
                break;
            }
            case ">": {
                for (int i = currentPosition+1; i<ranges.size(); i++){
                    result.add(i);
                }
                break;
            }
            case ">=": {
                for (int i = currentPosition; i<ranges.size(); i++){
                    result.add(i);
                }
                break;
            }
            case "=": {
                result.add(currentPosition);
                break;
            }
            case "!=": {
                for (int i = 0; i<ranges.size(); i++){
                    if(i!=currentPosition)
                        result.add(i);
                }
                break;
            }
            default:
                throw new DBAppException("Wrong Condition Operator!");
        }
        return result;
    }
    public Hashtable<SQLTerm,Vector<Hashtable<String,Object>>> loopBuckets (Vector<String> bucket_Paths,SQLTerm [] conditions) {
        Hashtable<SQLTerm, Vector<Hashtable<String, Object>>> result = new Hashtable<SQLTerm, Vector<Hashtable<String,Object>>>();
        for (int j = 0; j<conditions.length; j++) {
            Vector<String> targetVector = new Vector<String>();//path of pages inside the bucket
            SQLTerm[] test = new SQLTerm[conditionsOfIndexedColumns.size()];
            for (int i = 0; i < bucket_Paths.size(); i++) {
                Bucket currentBucket = Bucket.DeserializeBucket(bucket_Paths.get(i));
                (currentBucket.list).forEach((key, value) -> targetVector.addAll(value));
                //HSet<String> targetSet= new Set();
                Set<String> set = new HashSet<>();
                set.addAll(targetVector);
                targetVector.clear();
                targetVector.addAll(set);

            }
            result.putAll(loopPagesWithConditionforindexedColumns(targetVector, conditions[j]));
        }
        return result;
    }
    //////////////////////////////////////
    public Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> loopPagesWithConditionforindexedColumns(Vector<String> pages_paths, SQLTerm condition){
        Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> finalresult = new Hashtable<SQLTerm,Vector<Hashtable<String, Object>>>();
        // each condition with the rows satisifying the condition
        try {
            Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
            Object Value = condition._objValue;

            for (int j = 0; j < pages_paths.size(); j++) {
                //result = loopPageWithCondition(tableName, i, table.pagesPath.get(i), columnNames[i], conditions);
                Page page=deserialize(pages_paths.get(j));
                String columnName=condition._strColumnName;
                for (int j1 = 0; j < page.clusterings.size(); j++) {
                    Hashtable row = page.list.get(j);
                    if (row.get(columnName) != null) {
                        switch (condition._strOperator) {
                            case "<": {
                                if ((Double) (row.get(columnName)) < (Double) Value)
                                    result.add(row);
                                break;
                            }
                            case "<=": {
                                if ((Double) (row.get(columnName)) <= (Double) Value)
                                    result.add(row);
                                break;
                            }
                            case ">": {
                                if ((Double) (row.get(columnName)) > (Double) Value)
                                    result.add(row);
                                break;
                            }
                            case ">=": {
                                if ((Double) (row.get(columnName)) >= (Double) Value)
                                    result.add(row);
                                break;
                            }
                            case "=": {
                                if ((row.get(columnName)).equals(Value))
                                    result.add(row);
                                break;
                            }
                            case "!=": {
                                if (!(row.get(columnName)).equals(Value))
                                    result.add(row);
                                break;
                            }
                            default:
                                throw new DBAppException("Wrong Condition Operator!");
                        }
                    }
                }


                serializePage(page);
            }
            finalresult.put(condition,result);


        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }

        return finalresult;
    }

    //////////////////////////////////


    //    public Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> loopPagesWithCondition(String tableName, String[] columnNames,Vector<SQLTerm> conditions){
//        Table table =DeserializeTable("src/main/resources/data/" + tableName + ".bin");
//        Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> result = new Hashtable<SQLTerm,Vector<Hashtable<String, Object>>>();
//        // each condition with the rows satisifying the condition
//        try{
//            for(int i=0;i<table.pagesPath.size();i++){
//                result = loopPageWithCondition(tableName, i, table.pagesPath.get(i), columnNames[i], conditions);
//            }
//        }
//        catch(Exception e){
//            System.out.println(e.getMessage());
//        }
//        serializeTable(table);
//
//        return result;
//    }
//   public Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> loopPageWithCondition(String tableName, int pageNumber, String filepath, String columnName, Vector<SQLTerm> conditions) throws DBAppException{
//       Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> finalResult = new Hashtable<SQLTerm,Vector<Hashtable<String, Object>>>();
//       Page page=deserialize(filepath);
//       for(int i=0;i<conditions.size();i++) {
//           Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();
//           // vector of rows satisfying this condition
//           Object Value = conditions.get(i)._objValue;
//           for (int j = 0; j < page.clusterings.size(); j++) {
//               Hashtable row = page.list.get(j);
//               if (row.get(columnName) != null) {
//                   switch (conditions.get(i)._strOperator) {
//                       case "<": {
//                           if ((Double) (row.get(columnName)) < (Double) Value)
//                               result.add(row);
//                           break;
//                       }
//                       case "<=": {
//                           if ((Double) (row.get(columnName)) <= (Double) Value)
//                               result.add(row);
//                           break;
//                       }
//                       case ">": {
//                           if ((Double) (row.get(columnName)) > (Double) Value)
//                               result.add(row);
//                           break;
//                       }
//                       case ">=": {
//                           if ((Double) (row.get(columnName)) >= (Double) Value)
//                               result.add(row);
//                           break;
//                       }
//                       case "=": {
//                           if ((row.get(columnName)).equals(Value))
//                               result.add(row);
//                           break;
//                       }
//                       case "!=": {
//                           if (!(row.get(columnName)).equals(Value))
//                               result.add(row);
//                           break;
//                       }
//                       default:
//                           throw new DBAppException("Wrong Condition Operator!");
//                   }
//               }
//           }
//           Page overflow=new Page("name");
//           if (page.overflowPage != null) {
//               overflow = deserialize("src/main/resources/data" + "/" + tableName + pageNumber + "Over" + ".bin");
//               String p="src/main/resources/data" + "/" + tableName + i + "Over" + ".bin";  // overflow page path
//               for (int j = 0; j < page.clusterings.size(); j++) {
//                   Hashtable row = page.list.get(j);
//                   if (row.get(columnName) != null) {
//                       switch (conditions.get(i)._strOperator) {
//                           case "<": {
//                               if ((Double) (row.get(columnName)) < (Double) Value)
//                                   result.add(row);
//                               break;
//                           }
//                           case "<=": {
//                               if ((Double) (row.get(columnName)) <= (Double) Value)
//                                   result.add(row);
//                               break;
//                           }
//                           case ">": {
//                               if ((Double) (row.get(columnName)) > (Double) Value)
//                                   result.add(row);
//                               break;
//                           }
//                           case ">=": {
//                               if ((Double) (row.get(columnName)) >= (Double) Value)
//                                   result.add(row);
//                               break;
//                           }
//                           case "=": {
//                               if ((row.get(columnName)).equals(Value))
//                                   result.add(row);
//                               break;
//                           }
//                           case "!=": {
//                               if (!(row.get(columnName)).equals(Value))
//                                   result.add(row);
//                               break;
//                           }
//                           default:
//                               throw new DBAppException("Wrong Condition Operator!");
//                       }
//                   }}}
//           serializePage(overflow);
//           serializePage(page);
//
//           finalResult.put(conditions.get(i),result);
//       }
//       return finalResult;
//   }
//   ////////////////////
    public Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> loopPageWithConditionUsingIndex( String filepath,  Vector<SQLTerm> conditions) throws DBAppException{
        Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> finalResult = new Hashtable<SQLTerm,Vector<Hashtable<String, Object>>>();
        Page page=deserialize(filepath);
        for(int i=0;i<conditions.size();i++) {
            Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();
            Object Value = conditions.get(i)._objValue;
            String columnName=conditions.get(i)._strColumnName;
            for (int j = 0; j < page.clusterings.size(); j++) {
                Hashtable row = page.list.get(j);
                if (row.get(columnName) != null) {
                    switch (conditions.get(i)._strOperator) {
                        case "<": {
                            if ((Double) (row.get(columnName)) < (Double) Value)
                                result.add(row);
                            break;
                        }
                        case "<=": {
                            if ((Double) (row.get(columnName)) <= (Double) Value)
                                result.add(row);
                            break;
                        }
                        case ">": {
                            if ((Double) (row.get(columnName)) > (Double) Value)
                                result.add(row);
                            break;
                        }
                        case ">=": {
                            if ((Double) (row.get(columnName)) >= (Double) Value)
                                result.add(row);
                            break;
                        }
                        case "=": {
                            if ((row.get(columnName)).equals(Value))
                                result.add(row);
                            break;
                        }
                        case "!=": {
                            if (!(row.get(columnName)).equals(Value))
                                result.add(row);
                            break;
                        }
                        default:
                            throw new DBAppException("Wrong Condition Operator!");
                    }
                }
            }

            serializePage(page);

            finalResult.put(conditions.get(i),result);
        }
        return finalResult;
    }
    public Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> loopPagesWithCondition(String tableName, SQLTerm [] conditions){
        Table table =DeserializeTable("src/main/resources/data/" + tableName + ".bin");
        Hashtable<SQLTerm,Vector<Hashtable<String, Object>>> finalresult = new Hashtable<SQLTerm,Vector<Hashtable<String, Object>>>();
        // each condition with the rows satisifying the condition
        try {
            for (int i = 0; i < conditions.length; i++) {
                Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
                // vector of rows satisfying this condition
                Object Value = conditions[i]._objValue;

                for (int j = 0; j < table.pagesPath.size(); j++) {
                    //result = loopPageWithCondition(tableName, i, table.pagesPath.get(i), columnNames[i], conditions);
                    Page page=deserialize(table.pagesPath.get(j));
                    String columnName=conditions[i]._strColumnName;
                    for (int j1 = 0; j < page.clusterings.size(); j++) {
                        Hashtable row = page.list.get(j);
                        if (row.get(columnName) != null) {
                            switch (conditions[i]._strOperator) {
                                case "<": {
                                    if ((Double) (row.get(columnName)) < (Double) Value)
                                        result.add(row);
                                    break;
                                }
                                case "<=": {
                                    if ((Double) (row.get(columnName)) <= (Double) Value)
                                        result.add(row);
                                    break;
                                }
                                case ">": {
                                    if ((Double) (row.get(columnName)) > (Double) Value)
                                        result.add(row);
                                    break;
                                }
                                case ">=": {
                                    if ((Double) (row.get(columnName)) >= (Double) Value)
                                        result.add(row);
                                    break;
                                }
                                case "=": {
                                    if ((row.get(columnName)).equals(Value))
                                        result.add(row);
                                    break;
                                }
                                case "!=": {
                                    if (!(row.get(columnName)).equals(Value))
                                        result.add(row);
                                    break;
                                }
                                default:
                                    throw new DBAppException("Wrong Condition Operator!");
                            }
                        }
                    }
                    Page overflow=new Page("name");
                    if (page.overflowPage != null) {
                        overflow = deserialize("src/main/resources/data" + "/" + tableName + j + "Over" + ".bin");
                        //String p="src/main/resources/data" + "/" + tableName + i + "Over" + ".bin";  // overflow page path
                        for (int j3 = 0; j3 < page.clusterings.size(); j3++) {
                            Hashtable row = page.list.get(j);
                            if (row.get(columnName) != null) {
                                switch (conditions[i]._strOperator) {
                                    case "<": {
                                        if ((Double) (row.get(columnName)) < (Double) Value)
                                            result.add(row);
                                        break;
                                    }
                                    case "<=": {
                                        if ((Double) (row.get(columnName)) <= (Double) Value)
                                            result.add(row);
                                        break;
                                    }
                                    case ">": {
                                        if ((Double) (row.get(columnName)) > (Double) Value)
                                            result.add(row);
                                        break;
                                    }
                                    case ">=": {
                                        if ((Double) (row.get(columnName)) >= (Double) Value)
                                            result.add(row);
                                        break;
                                    }
                                    case "=": {
                                        if ((row.get(columnName)).equals(Value))
                                            result.add(row);
                                        break;
                                    }
                                    case "!=": {
                                        if (!(row.get(columnName)).equals(Value))
                                            result.add(row);
                                        break;
                                    }
                                    default:
                                        throw new DBAppException("Wrong Condition Operator!");
                                }
                            }}}
                    serializePage(overflow);
                    serializePage(page);
                }
                finalresult.put(conditions[i],result);


            }
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
        serializeTable(table);

        return finalresult;
    }


    public static void main (String[] args) throws DBAppException, IOException, ParseException {
        String [] minDate = "22-7-1999".split("-");
        String [] maxDate = "30-5-2014".split("-");
        Index i = new Index("courses", new String[]{"gpa"});
        buildArray(2, i.grid);
        Vector c = (Vector) i.grid.get(0);
        c.add(9);
        i.grid.set(0,c);
        // Vector y = (Vector) x.get(0);

        System.out.println(i.grid);

        //    public static void createRangeList (Index index ,String colName, String type, String min,String max){
        // Index i = new Index("student", new String[]{"name"});
        // createRangeList(i,"name","java.util.Date","1999-11-01","2012-11-01");
        // System.out.println(i.ranges.get("name").toString());
        //System.out.println(Integer.parseInt("12344"));
       /*FileWriter csvWriter = null;
        Table t = DeserializeTable("src/main/resources/data/pcs.bin");
        //System.out.println(readConfig("MaximumRowsCountinPage"));
        Page p = deserialize("src/main/resources/data/students1.bin");
       System.out.println(t.min.toString());
       System.out.println(t.max.toString());
        for(int i =0;i<p.list.size();i++){
            try {
                csvWriter = new FileWriter("src/main/resources/t.csv" ,true);

            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                csvWriter.write(p.list.get(i).toString());
                csvWriter.write('\n');
                csvWriter.flush();
                csvWriter.close();
            }
            catch (IOException ex){
                System.out.println(ex.getMessage());}
        }
        System.out.println(t.max.get(0));*/


    }}
