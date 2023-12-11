import java.io.*;
import java.util.*;

/*
 * This class is used to help create the metadata tables and user data inside a parent directory. 
 */
public class Directory {

    public static void createDirectory(File dataDirectory) throws IOException {
        try{
             //check if the dataDirectory folder can be created
            if (dataDirectory.mkdir()){
                createDataDirectory();
            }
        }catch (SecurityException e) {
            System.out.println("Unable to create data super directory");
        }
    }

    /*
     * This method takes the user data directory file and will make a directory folder out of it
     */
    public static void createDataDirectory() throws IOException {
        try{
            //Create necessary directories for the database catalog and user data
            File catalogDir = new File(Settings.getCatalogDirectory());
            File userDataDir = new File(Settings.getUserDataDirectory());
            
            //Check and create database catalog directory if it doesn't exist
            if (!catalogDir.exists()){
                if (!catalogDir.mkdir()){
                    System.out.println("Unable to create catalog directory");
                }
            }

            //Check and create user data directory if it doesn't exist
            if (!userDataDir.exists()){
                if (!userDataDir.mkdir()){
                    System.out.println("Unable to create user data directory");
                }
            }

            //ensure the catalog subdirectories are not null
            String[] existingFiles = catalogDir.list();
            assert existingFiles != null;

            //Delete existing files in the database catalog directory
            for (String tableName : existingFiles){
                File deletedFile =  new File(catalogDir, tableName);
                if (!deletedFile.delete()){
                    System.out.println("Unable to delete file");
                }
            }


        }catch (SecurityException e){
            System.out.println("Unable to create data sub directory");
        }

        // create new argonbase_tables and argonbase_columns tables
        createCatalogTables();
    }


    //create the database catalog directory if it exists
    public static void createCatalogDirectory() throws IOException {

        File catalog_dir = new File(Settings.getCatalogDirectory());

        if (catalog_dir.exists()){
            createCatalogTables();
        }

    }



     //create new argonbase tables table and columns table
    public static void createCatalogTables() throws IOException {
        //append the .tbl extension to all table files
        File argonBaseTablesFile = new File(Settings.getCatalogDirectory() + "/" + Settings.argonBaseTables + ".tbl");
        //check if the argonBase_tables table exists
        boolean isTableExist = argonBaseTablesFile.exists();
       
       
        File argonBaseColumnsFile = new File(Settings.getCatalogDirectory() + "/" + Settings.argonBaseColumns + ".tbl");
        //check if the argonBase_columns table exists
        boolean isColumnExist = argonBaseColumnsFile.exists();


        // create meta data tables
        Table argonBaseColumns = new Table(
            //get the table name
            Settings.argonBaseColumns,
            //column names 
            new ArrayList<>(Arrays.asList(
                "table_name",
                "column_name",
                "data_type",
                "ordinal_position",
                "is_nullable",
                "column_key"
            )),
            //column types 
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
            //check if user table
            false 
        );

        Table argonBaseTables = new Table(
             // table name
            Settings.argonBaseTables,
            // column names
            new ArrayList<>(List.of("table_name")),
            // column types
            new ArrayList<>(List.of(Constants.DataTypes.TEXT)), 
            // column is nullable
            new ArrayList<>(List.of(false)),
            // check if user table
            false 
        );
        

        Table.tableTable = argonBaseTables;
        Table.columnTable = argonBaseColumns;

        if (!isTableExist) {
            // insert into tables metadata
            argonBaseTables.insertRecord(new ArrayList<>(List.of(Settings.argonBaseTables)));
            argonBaseTables.insertRecord(new ArrayList<>(List.of(Settings.argonBaseColumns)));
        }

        if (!isColumnExist) {
            //insert into column metadata
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseTables, "table_name",
                    "TEXT", (byte) 1, "No", "PRI")));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "table_name",
                    "TEXT", (byte) 1, "No", null)));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "column_name",
                    "TEXT", (byte) 2, "No", null)));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "data_type",
                    "TEXT", (byte) 3, "No", null)));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "ordinal_position",
                 "TINYINT", (byte) 4, "No", null)));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "is_nullable",
                    "TEXT", (byte) 5, "No", null)));
            argonBaseColumns.insertRecord(new ArrayList<>(Arrays.asList(Settings.argonBaseColumns, "column_key",
                    "TEXT", (byte) 6, "No", null)));
        }
    }

}
