import static java.lang.System.out;

import java.io.*;
import java.util.*;

/*
 * This class provides all the functionality to perform SQL database command query parsing
 */
public class Commands {

    /* This method determines what type of command the userCommand is and
	 * calls the appropriate method to parse the userCommand String. 
	 */
    public static void parseUserCommand(String userCommand) throws IOException {
        
        /* commandTokens is an array of Strings that contains one lexical token per array
         * element. The first token can be used to determine the type of command
         * The other tokens can be used to pass relevant parameters to each command-specific
         * method inside each case statement
         */
        ArrayList<String> commandTokens = commandStringToTokenList(userCommand);

        /*
         *  This switch handles a very small list of hard-coded commands from SQL syntax.
         *  You will want to rewrite this method to interpret more complex commands.
         */
        switch (commandTokens.get(0).toLowerCase()) {
            case "show":
                show(commandTokens);
                break;
            case "select":
                parseQuery(commandTokens);
                break;
            case "create":
                if (commandTokens.get(1).equalsIgnoreCase("index")) {
                    parseCreateIndex(commandTokens);
                } else {
                    parseCreateTable(commandTokens);
                }
                break;
            case "insert":
                parseInsert(commandTokens);
                break;
            case "delete": 
                parseDelete(commandTokens);
                break;
            case "update": 
                parseUpdate(commandTokens);
                break;
            case "drop": 
                dropTable(commandTokens);
                break;
            case "help":
                help();
                break;
            case "version":
                displayVersion();
                break;
            case "exit":
                Settings.setExit(true);
                break;
            default:
                System.out.println("I didn't understand the command: \"" + userCommand + "\"");
                break;
        }
    }

    public static void displayVersion() {
        System.out.println("ArgonBase Version " + Settings.getVersion());
        System.out.println(Settings.getCopyright());
    }

    //parses the create index command 
    public static void parseCreateIndex(ArrayList<String> commandTokens) throws IOException {
        //check if the command structure is valid
        if (commandTokens.size() < 6 || !commandTokens.get(3).equals("(") || !commandTokens.get(5).equals(")")) {
            System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        //create a new Table object for the specified table
        Table table = new Table(commandTokens.get(2).toLowerCase(), true);
        //create an index on the specified column
        table.createIndex(commandTokens.get(4).toLowerCase());
    }

    //parses the create table command
    public static void parseCreateTable(ArrayList<String> commandTokens) throws IOException {
        //Arrays to store information about each column in the new table

        //store the column names in the new table where each element is a column name
        ArrayList<String> columnNames = new ArrayList<>();

        //store the column datatypes in the new table
		//each element is a column datatype that corresponds to the column at the same index in columnNames
        ArrayList<Constants.DataTypes> columnTypes = new ArrayList<>();

        //stores values whether each column is part of the primary key
		//each element corresponds to the column at the same index in columnNames and checks if it's primary key
        ArrayList<Boolean> primaryKey = new ArrayList<>();

        //store values whether each column has a unique constraint
		//each element corresponds to the column at the same index in columnNames and check if it has a unique constraint
        ArrayList<Boolean> unique = new ArrayList<>();

        //stores values whether each column allows null values
		//each element corresponds to the column at the same index in columnNames and checks if null values are allowed
        ArrayList<Boolean> isNull = new ArrayList<>();

        //Extract the table name from the command string token list
        String tableFileName = commandTokens.get(2).toLowerCase();

        //Create Table objects for metadata tables
        Table metatable = Table.tableTable;
        Table metaColumns = Table.columnTable;

        //checks if the table already exists
        if (Table.isTableExist(tableFileName)) {
            System.out.println("Table already exists!");
            return;
        }

        //starting index for parsing column definitions in command tokens
        int iter = 3;

        //check the command structure is valid
        if (!commandTokens.get(iter++).equals("(")) {
            System.out.println("Invalid Syntax: \"(\" Expected. \nType \"help;\" to display supported commands.");
            return;
        }


        while (!commandTokens.get(iter).equals(")")) {
            //Temporary list to store column type and column name
            ArrayList<String> temp = new ArrayList<>();

            //Extract column type and column name until a comma or closing parenthesis is encountered
            while (!commandTokens.get(iter).trim().equals(",") && !commandTokens.get(iter).trim().equals(")")) {
                temp.add(commandTokens.get(iter));
                iter++;
            }

            //check if there are at least 2 elements in the temporary list
            if (temp.size() < 2) {
                System.out.println("Invalid Syntax: Both Type and Column name required. \nType \"help;\" to display supported commands.");
                return;
            }

            //flags to track column constraints
            boolean pri = false;
            boolean uni = false;
            boolean nullable = true;

            //extract column type and column name from the temporary list
            columnNames.add(temp.get(0).toLowerCase());
            columnTypes.add(strDataTypes(temp.get(1)));

            int it = 2;
            // Loop through additional constraints specified for the column
            while (it < temp.size()) {
                String constraint = temp.get(it);
                
                //Check for various constraints and update flags accordingly
                if (constraint.equalsIgnoreCase("PRIMARY_KEY")) { 
                    pri = true;
                    nullable = false;
                    it++;
                } else if (constraint.equalsIgnoreCase("UNIQUE")) {
                    uni = true;
                    it++;
                } else if (constraint.equalsIgnoreCase("NOT_NULL")) {  
                    nullable = false;
                    it++;
                } else {
                    System.out.println("Invalid Syntax: Unknown table constraint " + constraint + ".\nType \"help;\" to display supported commands.");
                    return;
                }
            }

            //Add constraint flags to corresponding lists
            primaryKey.add(pri);
            unique.add(uni);
            isNull.add(nullable);

            //move to the next token
            iter++;

            //check if we have reached the end of command tokens list
            if (iter >= commandTokens.size()){
                break;
            }
        }

        //create a .tbl file to contain table data
        Table table = new Table(tableFileName, columnNames, columnTypes, isNull, true);

        //insert an entry in the argonbase_tables meta-data for this new table.
        metatable.insertRecord(new ArrayList<>(List.of(tableFileName)));

        //Code to insert entries in the argonbase_columns meta data for each column in the new table.
        for (int i = 0; i < columnTypes.size(); i++) {
            String isNullable;
            String columnKey;
            //Determine the column key based on constraints
            if (primaryKey.get(i)){
                //Primary key constraint
                columnKey = "PRI";
            }
            else if (unique.get(i)){
                //Unique constraint
                columnKey = "UNI";
            }

            else {
                //No specific constraint, so Nullable
                columnKey = "NULL";
            }

            //Determine whether the column is nullable or not
            if (isNull.get(i))
                isNullable = "YES";
            else
                isNullable = "NO";

            //Insert metadata for the current column into argonbase_columns
            metaColumns.insertRecord(
                new ArrayList<>(
                    Arrays.asList(
                        //table name
                        tableFileName.toLowerCase(),
                        //column name
                        columnNames.get(i).toLowerCase(),
                        //column type
                        columnTypes.get(i).toString(),
                        //ordinal position of the column
                        (byte) (i + 1),
                        //nullable info	
                        isNullable,
                        //PRI, UNI, or NULL
                        columnKey
                    )
                )
            );
        }
        //If there is at least one primary key, create an index for that key
        if (primaryKey.contains(true)) {
            //Get the name of the first column marked as Primary Key
			//then create an index for the Primary Key column
            table.createIndex(columnNames.get(primaryKey.indexOf(true)));
        }
    }

    //parses the show command to display information such as table names
	//takes a commandTokens list parameter which is extracted from the user command
    public static void show(ArrayList<String> commandTokens) throws IOException {
        ArrayList<Record> result;

        //Check if the show command is for displaying tables
        if (commandTokens.get(1).equalsIgnoreCase("tables")) {
            //Get the metadata table for tables
            Table table = Table.tableTable;

            //Retrieve all records which are the table names
            result = table.searchTable(null, null, null);
            
            //Display the result using the display method
            Commands.displayRecords(table, result, new ArrayList<>(), true);
        } else {
            //Display an error message if the show command is incorrect
            System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
        }
    }

    //parses and executes INSERT queries to add a new row od data into a specified table
	//input is the list of command tokens extracted from the user query
    public static void parseInsert(ArrayList<String> commandTokens) throws IOException {
        //check if the command has an adequate number of tokens
        if (commandTokens.size() < 5) {
            System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        //check if the command starts with "INTO"
        if (!commandTokens.get(1).equalsIgnoreCase("into")) {
            System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        //get the table name and check if it exists
        String tableFileName = commandTokens.get(2).toLowerCase();
        if (!Table.isTableExist(tableFileName)) {
            System.out.println("Table " + tableFileName + " does not exist.");
            return;
        }

        //initialize the table
        Table table = new Table(tableFileName, true);

        //arrays to store column names and column values temorarily
        String[] values = new String[table.columnNames.size()];
        String[][] temp = new String[table.columnNames.size()][2];

        //check the command strcutre and populate temporary arrays
        if (!commandTokens.get(3).equals("(") && !commandTokens.get(3).equalsIgnoreCase("values")) {
            out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        //checks if a list of column names were provided starting with open parenthesis
        if (commandTokens.get(3).equals("(")) {
            //iterate tokens starting from fifth token in command tokens
            int iter = 4;

            //keep track of number of columns encountered thru parsing
            int cptr = 0;

            //extract column names
            while (!commandTokens.get(iter).equals(")")) {
                if (!commandTokens.get(iter).equals(",")) {
                    temp[cptr++][0] = commandTokens.get(iter);
                }
                iter++;
            }
            //move to the "VALUES" part
            iter++;
            if (!commandTokens.get(iter).equalsIgnoreCase("values") ||
                    !commandTokens.get(iter + 1).equals("(")) {
                out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
                return;
            } else {
                iter += 2;
                cptr = 0;
                while (!commandTokens.get(iter).equals(")")) {
                    if (!commandTokens.get(iter).equals(",")) {
                        temp[cptr++][1] = commandTokens.get(iter);
                    }
                    iter++;
                }
            }

        } else if (commandTokens.get(3).equalsIgnoreCase("values")) {
            int iter = 4, vptr = 0;
            //check the command structure
            if (!commandTokens.get(iter).equals("(")) {
                out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
                return;
            } else {
                iter++;
                while (!commandTokens.get(iter).equals(")")) {
                    if (!commandTokens.get(iter).equals(",")) {
                        temp[vptr][0] = table.columnNames.get(vptr);
                        temp[vptr++][1] = commandTokens.get(iter);
                    }
                    iter++;
                }
            }
        }

        // create an array of values at appropriate positions
        for (String[] strings : temp) {
            if (strings[0] == null || strings[1] == null) {
                continue;
            }
            int j = table.columnNames.indexOf(strings[0]);
            if (j == -1) {
                out.println("Column " + strings[0] + " does not exist.");
                return;
            }
            values[j] = strings[1];
        }

        // check if each null value is nullable
        for (int flag = 0; flag < values.length; flag++) {
            if (values[flag] == null && !table.colIsNullable.get(flag)) {
                out.println(table.columnNames.get(flag) + " can not be NULL!");
                return;
            }
        }

        //parse values and perform insertion
        ArrayList<Object> insertValues = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                Constants.DataTypes type = table.columnTypes.get(i);
                Object value = DataTools.parseStr(type, values[i]);
                insertValues.add(value);
            } else
                insertValues.add(null);
        }

        //perform insertion and display result
        if (table.insertRecord(insertValues)) {
            out.println("1 row inserted successfully.");
        } else {
            out.println("Insertion failed.");
        }
    }

    //parses and executes DELETE queries to remove rows from a specified table based on specified conditions
	//input is the list of command tokens extract from user command
    public static void parseDelete(ArrayList<String> commandTokens) throws IOException {
        String columnName;
        Object value;
        String operator;

        //Check if the command is in the correct format
        if (!commandTokens.get(0).equalsIgnoreCase("delete") || !commandTokens.get(1).equalsIgnoreCase("from")) {
            out.println("Command is Invalid");
            return;
        }

        //Get the table name and check if it exists
        String tableName = commandTokens.get(2).toLowerCase();
        if (!Table.isTableExist(tableName)) {
            out.println("Table " + tableName + " does not exist.");
            return;
        }

        //Initialize the table
        Table table = new Table(tableName, true);
        int queryLength = commandTokens.size();

        //Check if a WHERE clause is present
        if (queryLength > 3) {
            if (!commandTokens.get(3).equalsIgnoreCase("where")) {
                out.println("Command is InValid");
                return;
            }

            //check if the query has the correct structure
            if (queryLength != 7 && queryLength != 8) {
                System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                return;
            }

            //parse the where clause
            if (commandTokens.get(4).equalsIgnoreCase("not")) {
                columnName = commandTokens.get(5).toLowerCase();
                operator = inverseOperator(commandTokens.get(6));
                Constants.DataTypes type = table.getColumnType(columnName);
                value = DataTools.parseStr(type, commandTokens.get(7));
            } else {
                columnName = commandTokens.get(4).toLowerCase();
                operator = commandTokens.get(5);
                Constants.DataTypes type = table.getColumnType(columnName);
                value = DataTools.parseStr(type, commandTokens.get(6));
            }
        } else {
            //No WHERE clause, set values to null
            columnName = null;
            value = null;
            operator = null;
        }

        //perform deletion and display result
        int deletedRows = table.deleteRecord(columnName, value, operator);
        if (deletedRows > 0){
            System.out.println(deletedRows + " rows are deleted!");
        }
        else{
            System.out.println("delete failed!");
        }
    }

    //parses and executes SELECT queries, retrieving data from the specific table based on given conditions
	//input parameter is the list of commandtokens extracted from the user command
    public static void parseQuery(ArrayList<String> commandTokens) throws IOException {
        //whether to return all columns of data or not
        boolean allColumns = false;
        String columnName;
        String value;
        String operator;
        ArrayList<String> columns = new ArrayList<>();
        String tableName;
        ArrayList<Record> result = new ArrayList<>();

        //check if the query is empty
        int queryLength = commandTokens.size();
        if (queryLength == 1) {
            System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        int i = 1;

        //check if all columns are selected using '*'
        if (commandTokens.get(i).equals("*")) {
            allColumns = true;
            i++;
        } else {
            //parse selected column names until 'FROM' is encountered
            while (i < queryLength && !(commandTokens.get(i).equalsIgnoreCase("from"))) {
                if (!(commandTokens.get(i).equalsIgnoreCase(","))) columns.add(commandTokens.get(i));
                i++;
            }
            //check if the query is incorrect
            if (i == queryLength) {
                System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                return;
            }
        }

        //move to the table name part of the query
        i++;
        if (i == queryLength) {
            System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        //get the table name and check if it exists
        tableName = commandTokens.get(i).toLowerCase();
        if (!Table.isTableExist(tableName)) {
            System.out.println("Table does not exist!");
            return;
        }

        //Initialize the table based on the table name
        Table table;
        try {
            if (tableName.equals(Settings.argonBaseTables) || tableName.equals(Settings.argonBaseColumns))
                table = new Table(tableName, false);
            else
                table = new Table(tableName, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        //move to the next part of the query
        i++;
        if (queryLength == i) {
            //if no additional conditions, then retrieve all records
            try {
                result = table.searchTable(null, null, null);
                
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (commandTokens.get(i).equalsIgnoreCase("where")) {
            //if WHERE clause is present
            i++;

            //check if the query length is correct for WHERE clause
            if (i + 3 == queryLength || i + 4 == queryLength) {
                if (commandTokens.get(i).equalsIgnoreCase("not")) {
                    //Handle "NOT" condition
                    columnName = commandTokens.get(i + 1);
                    value = commandTokens.get(i + 3);
                    operator = inverseOperator(commandTokens.get(i + 2));

                    //Check if the operator is valid
                    if (operator == null) {
                        System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                        return;
                    }
                } else {
                    //Handle regular condition
                    columnName = commandTokens.get(i);
                    operator = commandTokens.get(i + 1);
                    value = commandTokens.get(i + 2);
                }
                //Get the column type and parse the value
                Constants.DataTypes type = table.getColumnType(columnName);
                Object valueObject = DataTools.parseStr(type, value);

                //Search the table based on the condition
                result = table.searchTable(columnName.toLowerCase(), valueObject, operator);
            } else {
                System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                return;
            }
        }

        //Display the result of the query
        Commands.displayRecords(table, result, columns, allColumns);

    }

    //Drops and deletes a table and its associated files from the database
	//input the is list of command tokens extracted from the user command
    public static void dropTable(ArrayList<String> commandTokens) throws IOException {
        //check if the command is in the correct format
        if (!commandTokens.get(1).equalsIgnoreCase("table")) {
            out.println("Command is Invalid");
            return;
        }
        //Initialize the table to be dropped
        Table table = new Table(commandTokens.get(2).toLowerCase(), true);

        //Attempt to drop the table and display the result
        if (table.dropTable()) {
            
        } else {
            
        }
    }


    //parses and executes the update command, modifying data in the specified table
	//input is the list of command tokens extracted from the user command
    public static void parseUpdate(ArrayList<String> commandTokens) throws IOException {
        //variables necessary under WHERE clause
        String columnName;
        Object value;
        String operator;

        //variables to store column name and column value under SET clause
        String updateCol;
        Object updateVal;

        //Check if the command structure is in the correct format
        if (!commandTokens.get(0).equalsIgnoreCase("update") || !commandTokens.get(2).equalsIgnoreCase("set")) {
            out.println("Invalid Command Syntax");
            return;
        }

        //Extract table name from the command
        String tableName = commandTokens.get(1).toLowerCase();

        //Check if the specified table exists
        if (!Table.isTableExist(tableName)) {
            out.println("Table " + tableName + " does not exist.");
            return;
        }

        //Initialize the table for update operations
        Table table = new Table(commandTokens.get(1).toLowerCase(), true);

        //Extract column name and value from the SET clause
        updateCol = commandTokens.get(3);
        Constants.DataTypes updateColType = table.getColumnType(updateCol);
        updateVal = DataTools.parseStr(updateColType, commandTokens.get(5));

        //Determine the length of the command tokens array
        int queryLength = commandTokens.size();

        //check for the WHERE clause and parse its components
        if (queryLength > 6) {
            //Check if the WHERE clause is present and if the command length is correct
            if (!commandTokens.get(6).equalsIgnoreCase("where") && queryLength != 11 && queryLength != 10) {
                out.println("Invalid Command Syntax");
                return;
            }

            //check for NOT in WHERE clause and extract column name, column value, and operator
            if (commandTokens.get(5).equalsIgnoreCase("not")) {
                columnName = commandTokens.get(8).toLowerCase();
                Constants.DataTypes type = table.getColumnType(columnName);
                value = DataTools.parseStr(type, commandTokens.get(10));
                operator = inverseOperator(commandTokens.get(9));
                
                //check if the operator is valid
                if (operator == null) {
                    out.println("Invalid operator");
                    return;
                }
            } else {
                //extract column name, operator, and value from WHERE clause
                columnName = commandTokens.get(7).toLowerCase();
                Constants.DataTypes type = table.getColumnType(columnName);
                operator = commandTokens.get(8);
                value = DataTools.parseStr(type, commandTokens.get(9));
            }
        } else {
            //No WHERE clause provided
            columnName = null;
            operator = null;
            value = null;
        }

        //Perform the update operation and display the result
        int updated = table.updateTable(columnName, value, operator, updateCol, updateVal);
        if (updated > 0)
            System.out.println(updated + " rows updated!");
        else
            System.out.println("update failed!");
    }

    public static ArrayList<String> commandStringToTokenList(String command) {
        command = command.replaceAll("\n", " ");    // Remove newlines
        command = command.replaceAll("\r", " ");    // Remove carriage returns
        command = command.replaceAll(",", " , ");   // Tokenize commas
        command = command.replaceAll("\\(", " ( "); // Tokenize left parentheses
        command = command.replaceAll("\\)", " ) "); // Tokenize right parentheses
        command = command.replaceAll("( )+", " ");  // Reduce multiple spaces to a single space
        return new ArrayList<>(Arrays.asList(command.split(" ")));
    }


    //displays records return in a select query as a table
	//table is the name of the table
	//selectedColumns are the columns to be displayed as per the select query
	//allColumns is a boolean to know if there is a '*' wildcard in select query
    public static void displayRecords(Table table, ArrayList<Record> data, ArrayList<String> selColumns, boolean allColumns) {
        //Lists to store column numbers and their corresponding sizes for display
        ArrayList<Integer> columnNum = new ArrayList<>();
        ArrayList<Integer> colSize = new ArrayList<>();

        //determine which columns to display based on user input
        if (allColumns) {
            for (int i = 0; i < table.columnNames.size(); i++)
                columnNum.add(i);
        } else {
            for (String column : selColumns) {
                columnNum.add(table.columnNames.indexOf(column.toLowerCase()));
            }
        }

        //sort the column numbers for consistent display order
        Collections.sort(columnNum);


        //determine the maximum size of each column for proper alignment
        for (Integer i : columnNum) {
            int maxLength = table.columnNames.get(i).length();
            
            //Adjust the max length for specific data types
            Constants.DataTypes type = table.getColumnType(table.columnNames.get(i));
            switch (type) {
                case YEAR:
                    //year stores 4 digits in YYYY
                    maxLength = Math.max(4, maxLength);
                    break;
                case TIME:
                    //time stores 8 digits in HH:MM:SS 
                    maxLength = Math.max(8, maxLength);
                    break;
                case DATETIME:
                    //date time stores YYYY-MM-DD HH:MM:SS which is total of 19 digits
                    maxLength = Math.max(19, maxLength);
                    break;
                case DATE:
                    //date stores YYYY-MM-DD which is total of 10 digits 
                    maxLength = Math.max(10, maxLength);
                    break;
                default:
                    int temp = maxLength;
                    for (Record datum : data) {
                        int len;
                        Object val = datum.getValues().get(i);
                        if (val != null)
                            len = val.toString().trim().length();
                        else
                            len = 4;
                        temp = Math.max(len, temp);
                    }
                    maxLength = temp;
                    break;
            };
            colSize.add(maxLength);
        }

        //compute total length of the display, including separators
        int totalLength = (columnNum.size() - 1) * 3 + 4;
        //for (int i: colNum) - changed
        for (Integer integer : colSize) {
            totalLength += integer;
        }

        //print line separator
        System.out.println(Utils.printSeparator("-", totalLength));

        //print column names
        StringBuilder temp = new StringBuilder("|");
        for (Integer i : columnNum) {
            temp.append(" ").append(String.format("%-" + colSize.get(i) + "s", table.columnNames.get(i))).append(" |");
        }
        System.out.println(temp);


        //print a line
        System.out.println(Utils.printSeparator("-", totalLength));

        //print data records
        for (Record datum : data) {
            temp = new StringBuilder("|");
            for (Integer col : columnNum) {
                Constants.DataTypes type = table.getColumnType(table.columnNames.get(col));
                Object val = datum.getValues().get(col);
                String dataVal = DataTools.toStr(type, val);
                temp.append(" ").append(String.format("%-" + colSize.get(col) + "s", dataVal)).append(" |");
            }
            System.out.println(temp);
        }

        //print line separator
        System.out.println(Utils.printSeparator("-", totalLength));

    }

    //Converts a string of a data type to its corresponding data type from DataTypes enum and returns this
    public static Constants.DataTypes strDataTypes(String s) {
        Constants.DataTypes datatype;
		switch(s.toUpperCase()){
			case "INT":
				datatype = Constants.DataTypes.INT;
				break;
			case "TINYINT":
				datatype = Constants.DataTypes.TINYINT;
				break;
			case "SMALLINT":
				datatype = Constants.DataTypes.SMALLINT;
				break;
			case "BIGINT":
				datatype = Constants.DataTypes.BIGINT;
				break;
			case "FLOAT":
				datatype = Constants.DataTypes.FLOAT;
				break;
			case "DOUBLE":
				datatype = Constants.DataTypes.DOUBLE;
				break;
			case "YEAR":
				datatype = Constants.DataTypes.YEAR;
				break;
			case "TIME":
				datatype = Constants.DataTypes.TIME;
				break;
			case "DATETIME":
				datatype = Constants.DataTypes.DATETIME;
				break;
			case "DATE":
				datatype = Constants.DataTypes.DATE;
				break;
			case "TEXT":
				datatype = Constants.DataTypes.TEXT;
				break;
			default:
				datatype = Constants.DataTypes.NULL;
				break;
		}

		return datatype;

    }

    //Negates a comparison operator given the original comparison operator
    public static String inverseOperator(String operator) {
        String invertedOperator;
		switch(operator){
			case "=":
				invertedOperator = "!=";
				break;
			case "!=":
				invertedOperator = "=";
				break;
			case ">":
				invertedOperator = "<=";
				break;
			case "<":
				invertedOperator = ">=";
				break;
			case ">=":
				invertedOperator = "<";
				break;
			case "<=":
				invertedOperator = ">";
				break;
			default:
				invertedOperator = null;
				break;
		}

		return invertedOperator;
    }

    /**
     * Help: Display supported commands
     */
    public static void help() {
        out.println(Utils.printSeparator("*", 80));
        out.println("SUPPORTED COMMANDS\n");
        out.println("All commands below are case insensitive\n");
        out.println("SHOW TABLES;");
        out.println("\tDisplay the names of all tables.\n");
        out.println("CREATE TABLE <table_name> ( <column_name> <data_type> [NOT_NULL] [UNIQUE] [PRIMARY_KEY]);\n");
        out.println("\tCreates a table with the columns, datatypes, and constraints \n");
        out.println("SELECT column_list FROM table_name [WHERE condition];\n");
        out.println("\tDisplay table records whose optional condition \n");
        out.println("\tis <column_name> = <value>.\n");
        out.println("INSERT INTO (column1, column2, ...) table_name VALUES (value1, value2, ...);\n");
        out.println("\tInsert new record into the table. \n");
        out.println("INSERT INTO table_name VALUES (value1, value2, ...);\n");
        out.println("\tInsert new record into the table. \n");
        out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
        out.println("\tModify records data whose optional <condition> is\n");
        out.println("DROP TABLE table_name;");
        out.println("\tRemove table data (i.e. all records) and its schema.\n");
        out.println("DELETE FROM TABLE <table_name> [WHERE <condition>];\n");
        out.println("\tDelete records from a table given an optional WHERE condition");
        out.println("VERSION;");
        out.println("\tDisplay the program version.\n");
        out.println("HELP;");
        out.println("\tDisplay this help information.\n");
        out.println("EXIT;");
        out.println("\tExit the program.\n");
        out.println(Utils.printSeparator("*", 80));
    }
}
