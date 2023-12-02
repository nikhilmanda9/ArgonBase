import java.io.*;
import java.util.*;

public class Table {
    ArrayList<String> columnNames;
    ArrayList<Constants.DataTypes> columnTypes;
    ArrayList<Boolean> isNullable;
    String tableName;
    TableFile tableFile;
    String path;

    public static Table table_Table;
    public static Table column_Table;

    public Table(String tableName, ArrayList<String> columnNames, ArrayList<Constants.DataTypes> columnTypes, 
        ArrayList<Boolean> isNullable, boolean userData_Table){
         this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.isNullable = isNullable;
        if(userData_Table){
            this.path = Settings.getDataDirectory();
        }else{
            this.path = Settings.getCatalogDirectory();
        }

        try{
            tableFile = new TableFile(tableName, this.path);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    public Table(String tableName, boolean isUserTable) throws IOException{
        this.tableName = tableName;
        if(isUserTable){
            this.tableFile = new TableFile(tableName, Settings.getDataDirectory());
            this.path = Settings.getDataDirectory();
        }else{
            this.tableFile = new TableFile(tableName, Settings.getCatalogDirectory());
            this.path = Settings.getCatalogDirectory();
        }

        //load the table settings from metadata tables
        ArrayList<Record> tables = table_Table.searchTable("table_name", tableName, "=");
        if(tables.size() == 0){
            return;
        }

        if(tables.size() > 1){
            throw new RuntimeException("Multiple tables with same name exist");
        }

        ArrayList<Record> columns = column_Table.searchTable("table_name", tableName, "=");
        columnNames = new ArrayList<>();
        columnTypes = new ArrayList<>();
        isNullable = new ArrayList<>();
        for (Record col: columns){
            columnNames.add((String) col.getValues().get(1));
            columnTypes.add(Constants.DataTypes.valueOf((String) col.getValues().get(2)));
            isNullable.add(col.getValues().get(4) == "YES");
        }

    }

    //check if the index file exists
    public boolean isIndexExist(String columnName){
        File file = new File(path + "/" + tableName + "." + columnName + ".ndx");
        return file.exists();
    }

    public static boolean isTableExist(String tableName){
        ArrayList<Record> tables;
        try{
            tables = table_Table.searchTable("table_name", tableName, "=");
        }catch(IOException e){
            throw new RuntimeException(e);
        }

        return !tables.isEmpty();
    }

    //get the index file
    public IndexFile getIndexFile(String columnName){
        File indexFile = new File(path + "/" + tableName + "." + columnName + ".ndx");
        if(indexFile.exists()){
            try{
                return new IndexFile(this, columnName, path);
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


    //search table based on value and operator
    public ArrayList<Record> searchTable(String columnName, Object val, String op) throws IOException{
        //check if index exists for column name and if it does, use index to search
        //if index does not exist, use tableFile to search
        if(isIndexExist(columnName)){
            IndexFile iFile = getIndexFile(columnName);
            ArrayList<Integer> recordIds = iFile.search(val, op);
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
    public boolean insert(ArrayList<Object> values) throws IOException{
        ArrayList<Record> primaryKeys = column_Table.searchTable("column_key", "PRI", "=");
        primaryKeys.addAll(column_Table.searchTable("column_key", "UNI", "="));
        if(primaryKeys.size() > 0){
            for(Record rec: primaryKeys){
                if(rec.getValues().get(0).equals(tableName)){
                    String colName = (String) rec.getValues().get(1);
                    int colIndex = columnNames.indexOf(colName);
                    ArrayList<Record> duplicates = searchTable(colName, values.get(colIndex), "=");
                    if(duplicates.size() > 0){
                        System.out.println("Duplicate entry '" + values.get(colIndex) + "' for key'" + colName + "'");
                        return false;
                    }
                }
            }
        }

        int nextRowID = tableFile.getLastRowID() + 1;
        ArrayList<Constants.DataTypes> datatypes = new ArrayList<>(columnTypes);

        for(int i = 0; i < columnNames.size(); i++){
            if(values.get(i) == null){
                datatypes.set(i, Constants.DataTypes.NULL);
            }
        }

        Record rec = new Record(datatypes, values, nextRowID);
        tableFile.writeRecord(rec, tableFile.getLastLeafPage());

        //update indexes
        for(int k = 0; k < columnNames.size(); k++){
            if(isIndexExist(columnNames.get(k))){
                getIndexFile(columnNames.get(k)).addToCell(values.get(k), nextRowID);
            }
        }

        return true;
    }

    //delete records
    public int delete(String colName, Object val, String op) throws IOException{
        ArrayList<Record> records = searchTable(colName, val, op);
        for(Record rec: records){
            tableFile.deleteRecord(rec.getRowId());
            for(int k = 0; k < columnNames.size(); k++){
                if(isIndexExist(columnNames.get(k))){
                    getIndexFile(columnNames.get(k)).removeFromCell(rec.getValues().get(k), rec.getRowId());
                }
            }
        }

        return records.size();
    }

    //update records
    public int update(String searchCol, Object searchVal, String op, String updateCol, Object updateVal) throws IOException{
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
    public boolean dropTable(){
        try{
            table_Table.delete("table_name", this.tableName, "=");
            column_Table.delete("table_name", this.tableName, "=");
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        File tableFile = new File(path + "/" + tableName + ".tbl");
        return tableFile.delete();
    }




}

    
