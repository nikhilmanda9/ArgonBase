import java.io.*;
import java.util.*;

/*
 * This class provides all the functionality to perform SQL database command query parsing
 */
public class Commands {
	
	/* This method determines what type of command the userCommand is and
	 * calls the appropriate method to parse the userCommand String. 
	 */
	public static void parseUserCommand(String userCommand) throws IOException{
		System.out.println("Command: " + userCommand);

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
				//System.out.println("Case: SHOW");
				show(commandTokens);
				break;
			case "select":
				//System.out.println("Case: SELECT");
				parseQuery(commandTokens);
				break;
			case "create":
				//System.out.println("Case: CREATE");
				if(commandTokens.get(1).equalsIgnoreCase("index")){
					parseCreateIndex(commandTokens);
				}else{
					parseCreateTable(commandTokens);
				}
				break;
			case "insert":
				//System.out.println("Case: INSERT");
				parseInsert(commandTokens);
				break;
			case "delete":
				//System.out.println("Case: DELETE");
				parseDelete(commandTokens);
				break;
			case "update":
				//System.out.println("Case: UPDATE");
				parseUpdate(commandTokens);
				break;
			case "drop":
				//System.out.println("Case: DROP");
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

	//parses the create index command 
	public static void parseCreateIndex(ArrayList<String> commandTokens) throws IOException{
		//check if the command structure is valid
		if(commandTokens.size() < 6 || !commandTokens.get(3).equals("(") || !commandTokens.get(5).equals(")")){
			System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
			return;
		} 
		//create a new Table object for the specified table
		Table table = new Table(commandTokens.get(2).toLowerCase(), true);
		//create an index on the specified column
		table.createIndex(commandTokens.get(4).toLowerCase());
	}


	public static void displayVersion() {
		System.out.println("ArgonBase Version " + Settings.getVersion());
		System.out.println(Settings.getCopyright());
	}

	//parses the create table command
	public static void parseCreateTable(ArrayList<String> commandTokens) throws IOException{
		
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
		Table metadataTable = Table.table_Table;
		Table metadataColumns = Table.column_Table;

		//checks if the table already exists
		if(Table.isTableExist(tableFileName)){
			System.out.println("Table already exists");
			return;
		}
		
		int k = 3; //starting index for parsing column definitions in command tokens

		//check the command structure is valid
		if(!commandTokens.get(k).equals("(")){
			System.out.println("Invalid Syntax: \"(\" Expected. \nType \"help;\" to display supported commands.");
			return;
		}
		k++;
		
		while(!commandTokens.get(k).equals(")")){
			//Temporary list to store column type and column name
			ArrayList<String> colInfo = new ArrayList<>();

			//Extract column type and column name until a comma or closing parenthesis is encountered
			while(!commandTokens.get(k).trim().equals(",") && !commandTokens.get(k).trim().equals(")")){
				colInfo.add(commandTokens.get(k));
				k++;
			}
			//check if there are at least 2 elements in the temporary list
			if(colInfo.size() < 2){
				System.out.println("Invalid Syntax: Both Column Type and Column Name are required. \nType \"help;\" to display supported commands.");
				return;
			}
			//flags to track column constraints
			boolean isPriKey = false; //primary key constraint
			boolean isUniKey = false; //unique constraint
			boolean isNullable = true; //nullable constraint

			//extract column type and column name from the temporary list
			columnNames.add(colInfo.get(0).toLowerCase());
			columnTypes.add(strDataTypes(colInfo.get(1)));

			int j = 2;
			// Loop through additional constraints specified for the column
			while(j < colInfo.size()){
				String constraint = colInfo.get(j);
				
				//Check for various constraints and update flags accordingly
				if(constraint.equalsIgnoreCase("PRIMARY_KEY")){
					isPriKey = true;
					isNullable = false;
					j++;
				}else if(constraint.equalsIgnoreCase("UNIQUE")){
					isUniKey = true;
					j++;
				}
				else if(constraint.equalsIgnoreCase("NOT_NULL")){
					isNullable = false;
					j++;
				}else{
					//Invalid constraint detected
					System.out.println("Invalid Syntax: Unknown table constraint " + constraint + ".\nType \"help;\" to display supported commands.");
					return;
				}
			}
			//Add constraint flags to corresponding lists
			primaryKey.add(isPriKey);
			unique.add(isUniKey);
			isNull.add(isNullable);

			k++; //move to the next token

			//check if we have reached the end of command tokens list
			if(k >= commandTokens.size()){
				break;
			}
		}

		//create a .tbl file to contain table data
		Table table = new Table(tableFileName, columnNames, columnTypes, isNull, true);

		//insert an entry in the argonbase_tables meta-data for this new table.
		metadataTable.insertRecord(new ArrayList<>(List.of(tableFileName)));

		//Code to insert entries in the argonbase_columns meta data for each column in the new table.
		for(int i = 0; i < columnTypes.size(); i++){
			String isNullable;
			String columnKey;
			//Determine the column key based on constraints
			if(primaryKey.get(i)){
				columnKey = "PRI"; //Primary key constraint
			}else if(unique.get(i)){
				columnKey = "UNI"; //Unique constraint
			}else{
				columnKey = "NULL"; //No specific constraint, so Nullable
			}
			//Determine whether the column is nullable or not
			if(isNull.get(i)){
				isNullable = "YES";
			}else{
				isNullable = "NO";
			}
			//Insert metadata for the current column into argonbase_columns
			metadataColumns.insertRecord(
				new ArrayList<>(
					Arrays.asList(
						//table name
						tableFileName.toLowerCase(),
						//column name
						columnNames.get(i).toLowerCase(),
						//column type
						columnTypes.get(i).toString(),
						//ordinal position of the column
						(byte) (i+1),
						//nullable info	
						isNullable,
						//PRI, UNI, or NULL
						columnKey
					)
				)
			);
		}
		//If there is at least one primary key, create an index for that key
		if(primaryKey.contains(true)){
			//Get the name of the first column marked as Primary Key
			//then create an index for the Primary Key column
			table.createIndex(columnNames.get(primaryKey.indexOf(true)));
		}

	}

	//parses the show command to display information such as table names
	//takes a commandTokens list parameter which is extracted from the user command
	public static void show(ArrayList<String> commandTokens) throws IOException{
		ArrayList<Record> records;
		
		//Check if the show command is for displaying tables
		if(commandTokens.get(1).equalsIgnoreCase("tables")){
			//Get the metadata table for tables
			Table table = Table.table_Table;
			
			//Retrieve all records which are the table names
			records = table.searchTable(null, null, null);

			//Display the result using the display method
			Commands.displayRecords(table, records, new ArrayList<>(), true);
		}else{
			//Display an error message if the show command is incorrect
			System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
		}
	}

	//parses and executes INSERT queries to add a new row od data into a specified table
	//input is the list of command tokens extracted from the user query
	public static void parseInsert (ArrayList<String> commandTokens) throws IOException{
		//check if the command has an adequate number of tokens
		if(commandTokens.size() < 5){
			System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
			return;
		}

		//check if the command starts with "INTO"
		if(!commandTokens.get(1).equalsIgnoreCase("into")){
			System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
			return;
		}

		//get the table name and check if it exists
		String tableFileName = commandTokens.get(2).toLowerCase();
		if(!Table.isTableExist(tableFileName)){
			System.out.println("Table "+ tableFileName + " does not exist.");
			return;
		}

		//initialize the table
		Table table = new Table(tableFileName, true);

		//arrays to store column names and column values temorarily
		String[] colValues = new String[table.columnNames.size()];
		String[][] colNames = new String[table.columnNames.size()][2];

		//check the command strcutre and populate temporary arrays
		if(!commandTokens.get(3).equals("(") && !commandTokens.get(3).equalsIgnoreCase("values")){
			System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
			return;
		}

		//checks if a list of column names were provided starting with open parenthesis
		if(commandTokens.get(3).equals("(")){
			int k = 4; //iterate tokens starting from fifth token in command tokens
			int colCount = 0; //keep track of number of columns encountered thru parsing

			//extract column names
			while(!commandTokens.get(k).equals(")")){
				if(!commandTokens.get(k).equals(",")){
					colNames[colCount++][0] = commandTokens.get(k);
				}
				k++;
			}
			//move to the "VALUES" part
			k++;
			if(!commandTokens.get(k).equalsIgnoreCase("values") ||
				!commandTokens.get(k+1).equals("(")){
					System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
					return;
			} else{
				k += 2;
				colCount = 0;
				//extract values
				while(!commandTokens.get(k).equals(")")){
					if(!commandTokens.get(k).equals(",")){
						colNames[colCount++][1] = commandTokens.get(k);
					}
					k++;
				}
			}

		}else if(commandTokens.get(3).equalsIgnoreCase("values")){
			int k = 4;
			int valCount = 0;
			//check the command structure
			if(!commandTokens.get(k).equals("(")){
				System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
				return;
			} else{
				k++;
				//extract values
				while(!commandTokens.get(k).equals(")")){
					if(!commandTokens.get(k).equals(",")){
						colNames[valCount][0] = table.columnNames.get(valCount);
						colNames[valCount++][1] = commandTokens.get(k);
					}
					k++;
				}
			}
		}

		//create an array of values at appropriate positions
		for(String[] names: colNames){
			if(names[0] == null || names[1] == null){
				continue;
			}
			int i = table.columnNames.indexOf(names[0]);

			if(i == -1){
				System.out.println("Column " + names[0] + " does not exist.");
				return;
			}
			colValues[i] = names[1];
		}

		//check if each null value is nullable
		for(int i = 0; i < colValues.length; i++){
			if(colValues[i] == null && !table.isNullable.get(i)){
				System.out.println(table.columnNames.get(i) + " can not be NULL!");
				return;
			}
		}

		//parse values and perform insertion
		ArrayList<Object> rows = new ArrayList<>();
		for(int i = 0; i < colValues.length; i++){
			if(colValues[i] != null){
				Constants.DataTypes datatype = table.columnTypes.get(i);
				Object value = DataTools.parseStr(datatype, colValues[i]);
				rows.add(value);
			}else{
				rows.add(null);
			}
		}

		//perform insertion and display result
		if(table.insertRecord(rows)){
			System.out.println("1 row inserted successfully.");
		}else{
			System.out.println("Insertion failed.");
		}

	}
	
	//parses and executes DELETE queries to remove rows from a specified table based on specified conditions
	//input is the list of command tokens extract from user command
	public static void parseDelete(ArrayList<String> commandTokens) throws IOException{
		String columnName;
		Object value;
		String operator;

		//Check if the command is in the correct format
		if(!commandTokens.get(0).equalsIgnoreCase("delete") || !commandTokens.get(1).equalsIgnoreCase("from")){
			System.out.println("Command is invalid");
			return;
		}

		//Get the table name and check if it exists
		String tableName = commandTokens.get(2).toLowerCase();
		if(!Table.isTableExist(tableName)){
			System.out.println("Table " + tableName + " does not exist.");
			return;
		}

		//Initialize the table
		Table table = new Table(tableName, true);
		int query_size = commandTokens.size();

		//Check if a WHERE clause is present
		if(query_size > 3){
			if(!commandTokens.get(3).equalsIgnoreCase("where")){
				System.out.println("Command is invalid");
				return;
			}

			//check if the query has the correct structure
			if(query_size != 7 && query_size != 8){
				System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
				return;
			}

			//parse the where clause
			if(commandTokens.get(4).equalsIgnoreCase("not")){
				columnName = commandTokens.get(4).toLowerCase();
				operator = commandTokens.get(5);
				Constants.DataTypes datatype = table.getColumnType(columnName);
				value = DataTools.parseStr(datatype, commandTokens.get(7));
			}else{
				columnName = commandTokens.get(4).toLowerCase();
				operator = commandTokens.get(5);
				Constants.DataTypes datatype = table.getColumnType(columnName);
				value = DataTools.parseStr(datatype, commandTokens.get(6));
			}
		}else{
			//No WHERE clause, set values to null
			columnName = null;
			value = null;
			operator = null;
		}

		//perform deletion and display result
		int deletedRows = table.deleteRecord(columnName, value, operator);
		if(deletedRows > 0){
			System.out.println(deletedRows + " rows are deleted!");
		}else{
			System.out.println("delete failed!");
		}
	}
	

	//Drops and deletes a table and its associated files from the database
	//input the is list of command tokens extracted from the user command
	public static void dropTable(ArrayList<String> commandTokens) throws IOException{
		//check if the command is in the correct format
		if(!commandTokens.get(1).equalsIgnoreCase("table")){
			System.out.println("Command is Invalid");
			return;
		}
		//Initialize the table to be dropped
		Table table = new Table(commandTokens.get(2).toLowerCase(), true);
		
		//Attempt to drop the table and display the result
		if(table.dropTable()){
			System.out.println("Table " + table.tableName + " is dropped.");
		}else{
			System.out.println("Table " + table.tableName + " could not be dropped.");
		}
	}



	//parses and executes SELECT queries, retrieving data from the specific table based on given conditions
	//input parameter is the list of commandtokens extracted from the user command
	public static void parseQuery(ArrayList<String> commandTokens) throws IOException{
		boolean showAllColumns = false; //whether to return all columns of data or not
		String columnName;
		String value;
		String operator;
		ArrayList<String> columns = new ArrayList<>();
		String tableName;
		ArrayList<Record> records = new ArrayList<>();
		
		//check if the query is empty
		int query_size = commandTokens.size();
		if(query_size == 1){
			System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
			return;
		}

		int i = 1;

		//check if all columns are selected using '*'
		if(commandTokens.get(i).equals("*")){
			showAllColumns = true;
			i++;
		}else{
			//parse selected column names until 'FROM' is encountered
			while(i < query_size && !(commandTokens.get(i).equalsIgnoreCase("from"))){
				if(!(commandTokens.get(i).equalsIgnoreCase((",")))){
					columns.add(commandTokens.get(i));
				}
				i++;
			}

			//check if the query is incorrect
			if(i == query_size){
				System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
				return;
			}
		}

		//move to the table name part of the query
		i++;
		if(i == query_size){
			System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
			return;
		}

		//get the table name and check if it exists
		tableName = commandTokens.get(i).toLowerCase();
		if(!Table.isTableExist(tableName)){
			System.out.println("Table does not exist");
			return;
		}

		//Initialize the table based on the table name
		Table table;
		try{
			if(tableName.equals(Settings.argonBaseTables) || tableName.equals(Settings.argonBaseColumns)){
				table = new Table(tableName, false);
			}else{
				table = new Table(tableName, true);
			}
		}catch(Exception e){
			throw new RuntimeException(e);
		}

		//move to the next part of the query
		i++;
		if(query_size == i){
			//if no additional conditions, then retrieve all records
			try{
				records = table.searchTable(null, null, null);
			} catch(Exception e){
				throw new RuntimeException(e);
			}
		}else if (commandTokens.get(i).equalsIgnoreCase("where")){
			//if WHERE clause is present
			i++;

			//check if the query length is correct for WHERE clause
			if(i + 3 == query_size || i + 4 == query_size){
				if(commandTokens.get(i).equalsIgnoreCase("not")){
					//Handle "NOT" condition
					columnName = commandTokens.get(i+1);
					value = commandTokens.get(i+3);
					operator = inverseOperator(commandTokens.get(i+2));

					//Check if the operator is valid
					if(operator == null){
						System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
						return;
					}
				} else{
					//Handle regular condition
					columnName = commandTokens.get(i);
					operator = commandTokens.get(i+1);
					value = commandTokens.get(i+2);
				}
				//Get the column type and parse the value
				Constants.DataTypes datatype = table.getColumnType(columnName);
				Object valueObject = DataTools.parseStr(datatype, value);
				
				//Search the table based on the condition
				records = table.searchTable(columnName.toLowerCase(), valueObject, operator);
			}else{
				System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
				return;
			}
		}

		//Display the result of the query
		Commands.displayRecords(table, records, columns, showAllColumns);
	}

	//parses and executes the update command, modifying data in the specified table
	//input is the list of command tokens extracted from the user command
	public static void parseUpdate(ArrayList<String> commandTokens) throws IOException {
		//variables necessary under WHERE clause
		String colName;
		Object val;
		String operator;

		//variables to store column name and column value under SET clause
		String updateColumn;
		Object updateValue;

		//Check if the command structure is in the correct format
		if(!commandTokens.get(0).equalsIgnoreCase("update") || !commandTokens.get(2).equalsIgnoreCase("set")){
			System.out.println("Invalid Command Syntax");
			return;
		}

		//Extract table name from the command
		String tableName = commandTokens.get(1).toLowerCase();

		//Check if the specified table exists
		if(!Table.isTableExist(tableName)){
			System.out.println("Table " + tableName + " does not exist.");
			return;
		}

		//Initialize the table for update operations
		Table table = new Table(commandTokens.get(1).toLowerCase(), true);

		//Extract column name and value from the SET clause
		updateColumn = commandTokens.get(3);
		Constants.DataTypes updateColumnType = table.getColumnType(updateColumn);
		updateValue = DataTools.parseStr(updateColumnType, commandTokens.get(5));

		//Determine the length of the command tokens array
		int query_size = commandTokens.size();

		//check for the WHERE clause and parse its components
		if(query_size > 6){
			//Check if the WHERE clause is present and if the command length is correct
			if(!commandTokens.get(6).equalsIgnoreCase("where") && query_size != 11 && query_size != 10){
				System.out.println("Invalid Command Syntax");
				return;
			}

			//check for NOT in WHERE clause and extract column name, column value, and operator
			if(commandTokens.get(7).equalsIgnoreCase(("not"))){
				colName = commandTokens.get(8).toLowerCase();
				Constants.DataTypes datatype = table.getColumnType(colName);
				val = DataTools.parseStr(datatype, commandTokens.get(10));
				operator = inverseOperator(commandTokens.get(9));
				
				//check if the operator is valid
				if(operator == null){
					System.out.println("Invalid operator");
					return;
				}
			}else{
				//extract column name, operator, and value from WHERE clause
				colName = commandTokens.get(7).toLowerCase();
				Constants.DataTypes datatype = table.getColumnType(colName);
				operator = commandTokens.get(8);
				val = DataTools.parseStr(datatype, commandTokens.get(9));
			}
		}else{
			//No WHERE clause provided
			colName = null;
			operator = null;
			val = null;
		}

		//Perform the update operation and display the result
		int updatedRowCount = table.updateTable(colName, val, operator, updateColumn, updateValue);
		if(updatedRowCount > 0){
			System.out.println(updatedRowCount + " rows updated!");
		}else{
			System.out.println("Update failed");
		}
	}

	public static String tokensToCommandString (ArrayList<String> commandTokens) {
		String commandString = "";
		for(String token : commandTokens)
			commandString = commandString + token + " ";
		return commandString;
	}
	
	//Parses the user command string and divides into tokens which is return as an arraylist
	public static ArrayList<String> commandStringToTokenList (String command) {
		command.replace("\n", " ");
		command.replace("\r", " ");
		command.replace(",", " , ");
		command.replace("\\(", " ( ");
		command.replace("\\)", " ) ");
		command = command.replaceAll("( )+", " ");  // Reduce multiple spaces to a single space
		ArrayList<String> tokenizedCommand = new ArrayList<String>(Arrays.asList(command.split(" ")));
		return tokenizedCommand;
	}

	//displays records return in a select query as a table
	//table is the name of the table
	//selectedColumns are the columns to be displayed as per the select query
	//allColumns is a boolean to know if there is a '*' wildcard in select query
	public static void displayRecords(Table table, ArrayList<Record> data, ArrayList<String> selectedColumns, boolean showAllColumns){
		//Lists to store column numbers and their corresponding sizes for display
		ArrayList<Integer> columnNums = new ArrayList<>();
		ArrayList<Integer> columnSizes = new ArrayList<>();

		//determine which columns to display based on user input
		if(showAllColumns){
			for(int i = 0; i < table.columnNames.size(); i++){
				columnNums.add(i);
			}
		}else{
			for(String column: selectedColumns){
				columnNums.add(table.columnNames.indexOf(column.toLowerCase()));
			}
		}

		//sort the column numbers for consistent display order
		Collections.sort(columnNums);

		//determine the maximum size of each column for proper alignment
		for(Integer i: columnNums){
			int maxColLength = table.columnNames.get(i).length();

			//Adjust the max length for specific data types
			Constants.DataTypes dataType = table.getColumnType(table.columnNames.get(i));
			switch(dataType){
				case YEAR:
					//year stores 4 digits in YYYY
					maxColLength = Math.max(4, maxColLength);
					break;
				case TIME:
					//time stores 8 digits in HH:MM:SS
					maxColLength = Math.max(8, maxColLength);
					break;
				case DATETIME:
					//date time stores YYYY-MM-DD HH:MM:SS which is total of 19 digits
					maxColLength = Math.max(19, maxColLength);
					break;
				case DATE:
					//date stores YYYY-MM-DD which is total of 10 digits
					maxColLength = Math.max(10, maxColLength);
					break;
				default:
					int temp = maxColLength;
					for(Record record: data){
						int length;
						Object val = record.getValues().get(i);
						if(val != null){
							length = val.toString().trim().length();
						}else{
							length = 4;
						}
						temp = Math.max(length,temp);
					}

					maxColLength = temp;
					break;
			}
			columnSizes.add(maxColLength);
		}

		//compute total length of the display, including separators
		int colSeparation = (columnNums.size() - 1) * 3 + 4;
		for(Integer integer: columnSizes){
			colSeparation += integer;
		}

		//print line separator
		System.out.println(Utils.printSeparator("-", colSeparation));

		//print column names
		StringBuilder tableBuilder = new StringBuilder("|");
		for(Integer i: columnNums){
			tableBuilder.append(" ").append(String.format("%-" + columnSizes.get(i) + "s", table.columnNames.get(i))).append(" |");
		}
		System.out.println(tableBuilder);

		//print line separator
		System.out.println(Utils.printSeparator("-", colSeparation));

		//print data records
		for(Record record: data){
			tableBuilder = new StringBuilder("|");
			for(Integer col: columnNums){
				Constants.DataTypes datatype = table.getColumnType(table.columnNames.get(col));
				Object val = record.getValues().get(col);
				String dataStr = DataTools.toStr(datatype, val);
				tableBuilder.append(" ").append(String.format("%-" + columnSizes.get(col) + "s", dataStr)).append(" |");

			}
			System.out.println(tableBuilder);
		}

		//print line separator
		System.out.println(Utils.printSeparator("-", colSeparation));

	}

	//Converts a string of a data type to its corresponding data type from DataTypes enum and returns this
	public static Constants.DataTypes strDataTypes(String s){
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
	public static String inverseOperator(String operator){
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
	 *  Help: Display supported commands
	 */
	public static void help() {
		System.out.println(Utils.printSeparator("*",80));
		System.out.println("SUPPORTED COMMANDS\n");
		System.out.println("All commands below are case insensitive\n");
		System.out.println("SHOW TABLES;");
		System.out.println("\tDisplay the names of all tables.\n");
		System.out.println("SELECT column_list FROM table_name [WHERE condition];\n");
		System.out.println("\tDisplay table records whose optional condition");
		System.out.println("\tis <column_name> = <value>.\n");
		System.out.println("INSERT INTO (column1, column2, ...) table_name VALUES (value1, value2, ...);\n");
		System.out.println("\tInsert new record into the table.");
		System.out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		System.out.println("\tModify records data whose optional <condition> is\n");
		System.out.println("DROP TABLE table_name;");
		System.out.println("\tRemove table data (i.e. all records) and its schema.\n");
		System.out.println("VERSION;");
		System.out.println("\tDisplay the program version.\n");
		System.out.println("HELP;");
		System.out.println("\tDisplay this help information.\n");
		System.out.println("EXIT;");
		System.out.println("\tExit the program.\n");
		System.out.println(Utils.printSeparator("*",80));
	}
	
}
