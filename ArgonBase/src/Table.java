import java.io.*;
import java.util.*;


/*
 * This class represents a table from a database and tools with working with tables.
 */
public class Table {
    //Lists to store column information
    ArrayList<String> columnNames;
    ArrayList<Constants.DataTypes> columnTypes;
    ArrayList<Boolean> colIsNullable;
    String tableName;
    TableFile tableFile; //handles table file operations
    String path;

    //tables for managing metadata table_Table
    public static Table tableTable;
    public static Table columnTable;

    //constructor to create a table instance
    public Table(String tableName, boolean userTable) throws IOException {
        this.tableName = tableName;
        //set tableFile and path based on userTable flag
        if (userTable) {
            this.tableFile = new TableFile(tableName, Settings.getUserDataDirectory());
            this.path = Settings.getUserDataDirectory();
        } else {
            this.tableFile = new TableFile(tableName, Settings.getCatalogDirectory());
            this.path = Settings.getCatalogDirectory();
        }
        //load the table settings from metadata tables
        //search for the table in the metadata
        loadTable(tableName);
    }

     //constructor to create a Table instance
    //tableName is the name of the table to be created
    //columnNames are the names of the columns for the table
    //columnTypes are the datatypes of the columns to be created
    //isNullable stores if null values are allowed for the column
    //userData_Table indicates whether the table is a user table or metadata table
    public Table(String tableName, ArrayList<String> columnNames, ArrayList<Constants.DataTypes> columnTypes,
                 ArrayList<Boolean> colIsNullable, boolean userDataTable) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.colIsNullable = colIsNullable;

        //set path based on userData_Table flag
        if (userDataTable){
            this.path = Settings.getUserDataDirectory();
        }  
        else{
            this.path = Settings.getCatalogDirectory();
        }
        //Initialize tableFile
        try {
            tableFile = new TableFile(tableName, this.path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //Check if a table with the given name exists
    public static boolean isTableExist(String tableName) {
        ArrayList<Record> tables;
        try {
            //search for the table in the table metadata
            tables = tableTable.searchTable("table_name", tableName, "=");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return !tables.isEmpty();
    }

    //loads table settings from metadata tables
    public void loadTable(String tableName) throws IOException {
        ArrayList<Record> tables = tableTable.searchTable("table_name", tableName, "=");
        if (tables.size() == 0) {
            return;
        }
        if (tables.size() > 1) {
            throw new RuntimeException("More than one table with the same name");
        }
        ArrayList<Record> columns = columnTable.searchTable("table_name", tableName, "=");
        columnNames = new ArrayList<>();
        columnTypes = new ArrayList<>();
        colIsNullable = new ArrayList<>();
        for (Record column : columns) {
            columnNames.add((String) column.getValues().get(1));
            columnTypes.add(Constants.DataTypes.valueOf((String) column.getValues().get(2)));
            colIsNullable.add(column.getValues().get(4) == "YES");
        }
    }

     //search table based on column, value and operator
    public ArrayList<Record> searchTable(String columnName, Object value, String operator) throws IOException {
        //check if index exists for column name and if it does, use index to search
        //if index does not exist, use tableFile to search
        if (isIndexExist(columnName)) {
            IndexFile indexFile = getIndexFile(columnName);
            ArrayList<Integer> recordIds = indexFile.search(value, operator);
            ArrayList<Record> records = new ArrayList<>();
            for (int recordId : recordIds) {
                records.add(tableFile.getRecord(recordId));
            }
            return records;
        } else {
            int columnIndex;
            if (columnName != null && columnNames.contains(columnName)) {
                columnIndex = columnNames.indexOf(columnName);
            } else if (columnName == null) {
                columnIndex = -1;
            } else {
                return new ArrayList<>();
            }
            return tableFile.search(columnIndex, value, operator);
        }
    }

    //get the index file if it exists
    public IndexFile getIndexFile(String columnName){
        File indexFile = new File(path + "/" + tableName + "." + columnName + ".ndx");
        if (indexFile.exists()) {
            try {
                return new IndexFile(this, columnName, path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    //get the type of column such as PRI, UNI, or NULL
    public Constants.DataTypes getColumnType(String columnName) {
        return columnTypes.get(columnNames.indexOf(columnName));
    }


    //insert values into the table and handle rowid generation
    //return true if insertion is successful, otherwise return false
    public boolean insertRecord(ArrayList<Object> values) throws IOException {
        //check for duplicate entries in primary or unique columns
        ArrayList<Record> primaryKeySearch = columnTable.searchTable("column_key", "PRI", "=");
        primaryKeySearch.addAll(columnTable.searchTable("column_key", "UNI", "="));
        if (primaryKeySearch.size() > 0) {
            for (Record record : primaryKeySearch) {
                if (record.getValues().get(0).equals(tableName)) {
                    String columnName = (String) record.getValues().get(1);
                    int columnIndex = columnNames.indexOf(columnName);
                    ArrayList<Record> search = searchTable(columnName, values.get(columnIndex), "=");
                    if (search.size() > 0) {
                        //duplicate entry found
                        System.out.println("Duplicate entry '" + values.get(columnIndex) + "' for key '" + columnName + "'");
                        return false;
                    }
                }
            }
        }
        //Generate the next rowID
        int nextRowId = tableFile.getLastRowId() + 1;

        //Set NULL type for columns with null values
        ArrayList<Constants.DataTypes> types = new ArrayList<>(columnTypes);

        
        for (int i = 0; i < columnNames.size(); i++) {
            if (values.get(i) == null) {
                types.set(i, Constants.DataTypes.NULL);
            }
        }

        //Create a new record and append it to the table file
        Record rec = new Record(types, values, nextRowId);
        tableFile.appendRecord(rec);

        //update indexes
        for (int i = 0; i < columnNames.size(); i++) {
            if (isIndexExist(columnNames.get(i))) {
                getIndexFile(columnNames.get(i)).addItemToCell(values.get(i), nextRowId);
            }
        }

        //insertion successful
        return true;
    }

    //Deletes rows from the table based on a column, value, and operator
    //colName is the column used to search for records to delete
    //val is the value to search for
    //op is the operator to use in search
    //return the number of deleted rows
    public int deleteRecord(String columnName, Object value, String operator) throws IOException {
        //Search for records to delete
        ArrayList<Record> records = searchTable(columnName, value, operator);
        //Delete each record and update indexes
        for (Record record : records) {
            tableFile.deleteRecord(record.getRowId());
            for (int i = 0; i < columnNames.size(); i++) {
                if (isIndexExist(columnNames.get(i))) {
                    getIndexFile(columnNames.get(i)).removeItemFromCell(record.getValues().get(i), record.getRowId());
                }
            }
        }
        return records.size();
    }

    //update rows in the table based on a search condition
    //searchCol is the column to condition the update on
    //searchVal is the value to condition the update on
    //op is the operator to condition the update on
    //updateCol is the column to update
    //updateVal is the new value to write
    //return the number of rows updated
    public int updateTable(String searchColumn, Object searchValue, String operator,
                      String updateColumn, Object updateValue) throws IOException {

        int columnIndex;
        if (columnNames.contains(updateColumn)) {
            columnIndex = columnNames.indexOf(updateColumn);
        } else {
            return 0;
        }
        ArrayList<Record> records = searchTable(searchColumn, searchValue, operator);
        for (Record record : records) {
            tableFile.updateRecord(record.getRowId(), columnIndex, updateValue);
            if (isIndexExist(updateColumn)) {
                IndexFile indexFile = getIndexFile(updateColumn);
                indexFile.addItemToCell(updateValue, record.getRowId());
                indexFile.removeItemFromCell(record.getValues().get(columnIndex), record.getRowId());
            }
        }
        return records.size();
    }

    //drop table and delete corresponding metadata and indexes
    //return true if the table is dropped, false otherwise
    public boolean dropTable() {
        //Delete metadata entries for the table
        try {
            tableTable.deleteRecord("table_name", this.tableName, "=");
            columnTable.deleteRecord("table_name", this.tableName, "=");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //Delete the table file
        File tableFile = new File(path + "/" + tableName + ".tbl");
        return tableFile.delete();
    }

    //check if the index file exists for a given column
    //return true if the index file exists, false otherwise
    public boolean isIndexExist(String columnName) {
        File file = new File(path + "/" + tableName + "." + columnName + ".ndx");
        return file.exists();
    }


    //create index file for a column
    public void createIndex(String columnName) {
        if (isIndexExist(columnName)) {
            return;
        }
        try (IndexFile indexFile = new IndexFile(this, columnName, path)) {
            indexFile.populateIndex();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
