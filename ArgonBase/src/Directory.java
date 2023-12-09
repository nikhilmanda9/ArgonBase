//import static java.lang.System.nanoTime;

import java.io.*;
import java.util.*;

/*
 * This class is used to help create the metadata tables and user data inside a parent directory. 
 */
public class Directory {

    /*
     * This method takes the user data directory file and will make a directory folder out of it
     */
    public static void createDataDirectory(File dataDirectory) throws IOException{
        try{
            //check if the userDatadirectory folder can be created
            if(dataDirectory.mkdir()){
                try{
                    //Create necessary directories for the database catalog and user data
                    File catalogDir = new File(Settings.getCatalogDirectory());
                    File userDataDir = new File(Settings.getUserDataDirectory());

                    //Check and create database catalog directory if it doesn't exist
                    if(!catalogDir.exists()){
                        if(!catalogDir.mkdir()){
                            System.out.println("Error: Unable to create catalog directory");
                        }
                    }

                    // Check and create user data directory if it doesn't exist
                    if(!userDataDir.exists()){
                        if (!userDataDir.mkdir()){
                            System.out.println("Error: Unable to create user data directory");
                        }
                    }
                    
                    //ensure the catalog subdirectories are not null
                    String[] existingFiles = catalogDir.list();
                    if(existingFiles == null){
                        throw new IllegalArgumentException("List of existing files cannot be null");
                    }

                    //Delete existing files in the database catalog directory
                    for(String tableName : existingFiles){
                        File deletedFile = new File(catalogDir, tableName);
                        if(!deletedFile.delete()){
                            System.out.println("Unable to delete table file");
                        }
                    }

                }catch(Exception e){
                    System.out.println("Unable to create data sub directory");
                }

                //create new argonbase_tables and argonbase_columns tables
                createCatalogTables();
            }
        }catch(Exception e){
            System.out.println("Unable to create data super directory");
        }
    }
    
    //create the database catalog directory if it exists
    public static void createCatalogDirectory() throws IOException{
        File catalogDir = new File(Settings.getCatalogDirectory());

        if(catalogDir.exists()){
            createCatalogTables();
        }
    }

    //create new argonbase tables table and columns table
    public static void createCatalogTables() throws IOException{
        //append the .tbl extension to all table files
        File argonBaseTablesFile = new File(Settings.getCatalogDirectory() + "/" + Settings.argonBaseTables + ".tbl");
        File argonBaseColumnsFile = new File(Settings.getCatalogDirectory() + "/" + Settings.argonBaseColumns + ".tbl");

        //check if the argonBase_columns tables and tables table exist
        boolean isColumnExist = argonBaseColumnsFile.exists();
        boolean isTableExist = argonBaseTablesFile.exists();

        //create tables table for metadata
        Table argonBaseTables = new Table(
            //get the table name
            Settings.argonBaseTables,
            //get the column names
            new ArrayList<>(List.of("table_name")),
            //get the column types
            new ArrayList<>(List.of(Constants.DataTypes.TEXT)),
            //column is nullable
            new ArrayList<>(List.of(false)),
            //check if user data table
            false
        );

        //create columns table for metadata, pass in the column names followed by column types
        Table argonBaseColumns = new Table(
           //get the table name
           Settings.argonBaseColumns,
           new ArrayList<>(Arrays.asList(
                "table_name",
                "column_name",
                "data_type",
                "ordinal_position",
                "is_nullable",
                "column_key"
           )),
           new ArrayList<>(Arrays.asList(
                Constants.DataTypes.TEXT,
                Constants.DataTypes.TEXT,
                Constants.DataTypes.TEXT,
                Constants.DataTypes.TINYINT,
                Constants.DataTypes.TEXT,
                Constants.DataTypes.TEXT
           )),
           //column is nullable
           new ArrayList<>(Arrays.asList(false,false,false,false,false,true)),
           //check if user data table
           false
        );
        
        Table.table_Table = argonBaseTables;
        Table.column_Table = argonBaseColumns;

        if(!isTableExist){
            //insert into tables metadata
            argonBaseTables.insertRecord(new ArrayList<>(List.of(Settings.argonBaseTables)));
            argonBaseTables.insertRecord(new ArrayList<>(List.of(Settings.argonBaseColumns)));
        }

        if(!isColumnExist){
            //insert into column metadata
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseTables, "table_name", "TEXT", (byte) 1, "No", "PRI")));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "table_name", "TEXT", (byte) 1, "No", null)));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "column_name", "TEXT", (byte) 2, "No", null)));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "data_type", "TEXT", (byte) 3, "No", null)));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "ordinal_position", "TINYINT", (byte) 4, "No", null)));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "is_nullable", "TEXT", (byte) 5, "No", null)));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "column_key", "TEXT", (byte) 6, "No", null)));
        }
        
    }

}
