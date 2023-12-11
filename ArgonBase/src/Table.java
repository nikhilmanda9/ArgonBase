import java.io.*;
import java.util.*;

/*
 * This class represents a table from a database and tools with working with tables.
 */
public class Table {
    //Lists to store column information
    ArrayList<String> columnNames;
    ArrayList<Constants.DataTypes> columnTypes;
    ArrayList<Boolean> isNullable;
    String tableName;
    TableFile tableFile; //handles table file operations
    String path;

    //tables for managing metadata table_Table
    public static Table table_Table;
    public static Table column_Table;

    //constructor to create a Table instance
    //tableName is the name of the table to be created
    //columnNames are the names of the columns for the table
    //columnTypes are the datatypes of the columns to be created
    //isNullable stores if null values are allowed for the column
    //userData_Table indicates whether the table is a user table or metadata table
    public Table(String tableName, ArrayList<String> columnNames, ArrayList<Constants.DataTypes> columnTypes, 
        ArrayList<Boolean> isNullable, boolean userData_Table){
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.isNullable = isNullable;

        //set path based on userData_Table flag
        if(userData_Table){
            this.path = Settings.getUserDataDirectory();
        }else{
            this.path = Settings.getCatalogDirectory();
        }
        //Initialize tableFile
        try{
            tableFile = new TableFile(tableName, this.path);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    //constructor to create a table instance
    public Table(String tableName, boolean isUserTable) throws IOException{
        this.tableName = tableName;
        //set tableFile and path based on userTable flag
        if(isUserTable){
            this.tableFile = new TableFile(tableName, Settings.getUserDataDirectory());
            this.path = Settings.getUserDataDirectory();
        }else{
            this.tableFile = new TableFile(tableName, Settings.getCatalogDirectory());
            this.path = Settings.getCatalogDirectory();
        }

        //load the table settings from metadata tables
        //search for the table in the metadata
        ArrayList<Record> tables = table_Table.searchTable("table_name", tableName, "=");
        if(tables.size() == 0){
            return; //table not found
        }

        if(tables.size() > 1){
            throw new RuntimeException("Multiple tables with same name exist");
        }
        //Load columns for the table
        ArrayList<Record> columns = column_Table.searchTable("table_name", tableName, "=");
        columnNames = new ArrayList<>();
        columnTypes = new ArrayList<>();
        isNullable = new ArrayList<>();
        for (Record col: columns){
            //Extract column information from metadata
            columnNames.add((String) col.getValues().get(1));
            columnTypes.add(Constants.DataTypes.valueOf((String) col.getValues().get(2)));
            isNullable.add(col.getValues().get(4) == "YES");
        }

    }

    //check if the index file exists for a given column
    //return true if the index file exists, false otherwise
    public boolean isIndexExist(String columnName){
        File file = new File(path + "/" + tableName + "." + columnName + ".ndx");
        return file.exists();
    }

    //Check if a table with the given name exists
    public static boolean isTableExist(String tableName){
        ArrayList<Record> tables;
        try{
            //search for the table in the table metadata
            tables = table_Table.searchTable("table_name", tableName, "=");
        }catch(IOException e){
            throw new RuntimeException(e);
        }

        return !tables.isEmpty();
    }

    //get the index file if it exists
    public IndexFile getIndexFile(String colName){
        File indexFile = new File(path + "/" + tableName + "." + colName + ".ndx");
        if(indexFile.exists()){
            try{
                return new IndexFile(this, colName, path);
            }catch(Exception e){
                throw new RuntimeException(e);
            }
        }

        return null;
    }

    //get the type of column such as PRI, UNI, or NULL
    public Constants.DataTypes getColumnType(String columnName){
        return columnTypes.get(columnNames.indexOf(columnName));
    }


    //search table based on column, value and operator
    public ArrayList<Record> searchTable(String columnName, Object val, String op) throws IOException{
        //check if index exists for column name and if it does, use index to search
        //if index does not exist, use tableFile to search
        if(isIndexExist(columnName)){
            IndexFile iFile = getIndexFile(columnName);
            ArrayList<Integer> recordIds = iFile.searchRowIDsInRange(val, op);
            ArrayList<Record> records = new ArrayList<>();
            for(int recId: recordIds){
                records.add(tableFile.getRecord(recId));
            }
            return records;
        }else{
            int colIndex;
            if(columnName != null && columnNames.contains(columnName)){
                colIndex = columnNames.indexOf(columnName);
            }else if(columnName == null){
                colIndex = -1;
            }else{
                return new ArrayList<>();
            }

            return tableFile.searchUsingCond(colIndex, val, op);
        }
    }

    //insert values into the table and handle rowid generation
    //return true if insertion is successful, otherwise return false
    public boolean insertRecord(ArrayList<Object> values) throws IOException{
        //check for duplicate entries in primary or unique columns
        ArrayList<Record> primaryKeys = column_Table.searchTable("column_key", "PRI", "=");
        primaryKeys.addAll(column_Table.searchTable("column_key", "UNI", "="));
        if(primaryKeys.size() > 0){
            for(Record rec: primaryKeys){
                if(rec.getValues().get(0).equals(tableName)){
                    String colName = (String) rec.getValues().get(1);
                    int colIndex = columnNames.indexOf(colName);
                    ArrayList<Record> duplicates = searchTable(colName, values.get(colIndex), "=");
                    if(duplicates.size() > 0){
                        //duplicate entry found
                        System.out.println("Duplicate entry '" + values.get(colIndex) + "' for key'" + colName + "'");
                        return false;
                    }
                }
            }
        }
        
        //Generate the next rowID
        int nextRowID = tableFile.getLastRowID() + 1;
        
        //Set NULL type for columns with null values
        ArrayList<Constants.DataTypes> datatypes = new ArrayList<>(columnTypes);

        for(int i = 0; i < columnNames.size(); i++){
            if(values.get(i) == null){
                datatypes.set(i, Constants.DataTypes.NULL);
            }
        }

        //Create a new record and append it to the table file
        Record rec = new Record(datatypes, values, nextRowID);
        tableFile.writeRecord(rec, tableFile.getLastLeafPage());

        //update indexes
        for(int k = 0; k < columnNames.size(); k++){
            if(isIndexExist(columnNames.get(k))){
                getIndexFile(columnNames.get(k)).addToCell(values.get(k), nextRowID);
            }
        }

        return true; //insertion successful
    }

    //Deletes rows from the table based on a column, value, and operator
    //colName is the column used to search for records to delete
    //val is the value to search for
    //op is the operator to use in search
    //return the number of deleted rows
    public int deleteRecord(String colName, Object val, String op) throws IOException{
        //Search for records to delete
        ArrayList<Record> records = searchTable(colName, val, op);
        //Delete each record and update indexes
        for(Record rec: records){
            tableFile.deleteRecord(rec.getRowId());
            for(int k = 0; k < columnNames.size(); k++){
                if(isIndexExist(columnNames.get(k))){
                    getIndexFile(columnNames.get(k)).removeFromCell(rec.getValues().get(k), rec.getRowId());
                }
            }
        }

        return records.size(); //return number of deleted rows
    }

    //update rows in the table based on a search condition
    //searchCol is the column to condition the update on
    //searchVal is the value to condition the update on
    //op is the operator to condition the update on
    //updateCol is the column to update
    //updateVal is the new value to write
    //return the number of rows updated
    public int updateTable(String searchCol, Object searchVal, String op, String updateCol, Object updateVal) throws IOException{
        int colIndex;
        if(columnNames.contains(updateCol)){
            colIndex = columnNames.indexOf(updateCol);
        }else{
            return 0;
        }

        ArrayList<Record> records = searchTable(searchCol, searchVal, op);
        for(Record rec: records){
            tableFile.updateRecord(rec.getRowId(), colIndex, updateVal);
            if(isIndexExist(updateCol)){
                IndexFile indexFile = getIndexFile(updateCol);
                indexFile.addToCell(updateVal, rec.getRowId());
                indexFile.removeFromCell(rec.getValues().get(colIndex), rec.getRowId());
            }
        }

        return records.size();
    }

    //create index file for a column
    public void createIndex(String colName){
        if(isIndexExist(colName)){
            return;
        }
        try(IndexFile indexFile = new IndexFile(this, colName, path)){
            indexFile.populateIndex();
            
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }


    //drop table and delete corresponding metadata and indexes
    //return true if the table is dropped, false otherwise
    public boolean dropTable(){
        //Delete metadata entries for the table
        try{
            table_Table.deleteRecord("table_name", this.tableName, "=");
            column_Table.deleteRecord("table_name", this.tableName, "=");
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        //Delete the table file
        File tableFile = new File(path + "/" + tableName + ".tbl");
        return tableFile.delete();
    }
}