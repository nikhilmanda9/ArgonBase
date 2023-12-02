import java.io.*;
import java.util.*;

public class Commands {
	
	/* This method determines what type of command the userCommand is and
	 * calls the appropriate method to parse the userCommand String. 
	 */
	public static void parseUserCommand (String userCommand) throws IOException{
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
			case "quit":
				Settings.setExit(true);
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}

	public static void parseCreateIndex(ArrayList<String> commandTokens) throws IOException{
		if(commandTokens.size() < 6 || !commandTokens.get(3).equals("(") || !commandTokens.get(5).equals(")")){
			System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
			return;
		} 

		Table table = new Table(commandTokens.get(2).toLowerCase(), true);
		table.createIndex(commandTokens.get(4).toLowerCase());
	}
	public static void displayVersion() {
		System.out.println("ArgonBase Version " + Settings.getVersion());
		System.out.println(Settings.getCopyright());
	}


	public static void parseCreateTable(ArrayList<String> commandTokens) throws IOException{
		
		// System.out.println("Stub: parseCreateTable method");
		// System.out.println("Command: " + command);
		
		ArrayList<String> columnNames = new ArrayList<>();
		ArrayList<Constants.DataTypes> columnTypes = new ArrayList<>();
		ArrayList<Boolean> primaryKey = new ArrayList<>();
		ArrayList<Boolean> unique = new ArrayList<>();
		ArrayList<Boolean> isNull = new ArrayList<>();

		/* Extract the table name from the command string token list */
		String tableFileName = commandTokens.get(2).toLowerCase();

		Table metadataTable = Table.table_Table;
		Table metadataColumns = Table.column_Table;
		if(Table.isTableExist(tableFileName)){
			System.out.println("Table already exists");
			return;
		}
		
		//parse the query
		int k = 3; //total number of command tokens
		if(!commandTokens.get(k++).equals("(")){
			System.out.println("Invalid Syntax: \"(\" Expected. \nType \"help;\" to display supported commands.");
			return;
		}

		while(!commandTokens.get(k).equals(")")){
			ArrayList<String> temp = new ArrayList<>();
			while(!commandTokens.get(k).trim().equals(",") && !commandTokens.get(k).trim().equals(")")){
				temp.add(commandTokens.get(k));
				k++;
			}

			if(temp.size() < 2){
				System.out.println("Invalid Syntax: Both Column Type and Column Name are required. \nType \"help;\" to display supported commands.");
				return;
			}

			boolean isPRI = false;
			boolean isUNI = false;
			boolean isNullable = true;
			columnNames.add(temp.get(0).toLowerCase());
			columnTypes.add(strDataTypes(temp.get(1)));
			int j = 2;
			while(j < temp.size()){
				String constraint = temp.get(j);
				//table constraint not null should be given as PRIMARY_KEY
				if(constraint.equalsIgnoreCase("PRIMARY_KEY")){
					isPRI = true;
					isNullable = false;
					j++;
				}else if(constraint.equalsIgnoreCase("UNIQUE")){
					isUNI = true;
					j++;
				}
				//table constraint not null should be given as NOT_NULL
				else if(constraint.equalsIgnoreCase("NOT_NULL")){
					isNullable = false;
					j++;
				}else{
					System.out.println("Invalid Syntax: Unknown table constraint " + constraint + ".\nType \"help;\" to display supported commands.");
					return;
				}
			}
			primaryKey.add(isPRI);
			unique.add(isUNI);
			isNull.add(isNullable);
			k++;
			if(k >= commandTokens.size()){
				break;
			}
		}

		/*  Code to create a .tbl file to contain table data */
		Table table = new Table(tableFileName, columnNames, columnTypes, isNull, true);

		/*  Code to insert an entry in the TABLES meta-data for this new table.
		 *  i.e. New row in argonbase_tables if you're using that mechanism for meta-data.
		 */
		metadataTable.insert(new ArrayList<>(List.of(tableFileName)));

		/*  Code to insert entries in the COLUMNS meta data for each column in the new table.
		 *  i.e. New rows in argonbase_columns if you're using that mechanism for meta-data.
		 */
		for(int i = 0; i < columnTypes.size(); i++){
			String isNullable;
			String columnKey;
			if(primaryKey.get(i)){
				columnKey = "PRI";
			}else if(unique.get(i)){
				columnKey = "UNI";
			}else{
				columnKey = "NULL";
			}

			if(isNull.get(i)){
				isNullable = "YES";
			}else{
				isNullable = "NO";
			}

			metadataColumns.insert(
				new ArrayList<>(
					Arrays.asList(
						tableFileName.toLowerCase(),
						columnNames.get(i).toLowerCase(),
						columnTypes.get(i).toString(),
						(byte) (i+1),
						isNullable,
						columnKey
					)
				)
			);
		}

		if(primaryKey.contains(true)){
			table.createIndex(columnNames.get(primaryKey.indexOf(true)));
		}

	}

	public static void show(ArrayList<String> commandTokens) throws IOException{
		ArrayList<Record> records;
		if(commandTokens.get(1).equalsIgnoreCase("tables")){
			Table table = Table.table_Table;
			records = table.searchTable(null, null, null);
			Commands.displayRecords(table, records, new ArrayList<>(), true);
		}else{
			System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
		}
	}

	
	public static void parseInsert (ArrayList<String> commandTokens) throws IOException{
		if(commandTokens.size() < 5){
			System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
			return;
		}

		if(!commandTokens.get(1).equalsIgnoreCase("into")){
			System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
			return;
		}

		String tableFileName = commandTokens.get(2).toLowerCase();
		if(!Table.isTableExist(tableFileName)){
			System.out.println("Table "+ tableFileName + " does not exist.");
			return;
		}

		Table table = new Table(tableFileName, true);
		String[] values = new String[table.columnNames.size()];
		String[][] temp_values = new String[table.columnNames.size()][2];
		if(!commandTokens.get(3).equals("(") && !commandTokens.get(3).equalsIgnoreCase("values")){
			System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
			return;
		}

		if(commandTokens.get(3).equals("(")){
			int k = 4;
			int cptr = 0;
			while(!commandTokens.get(k).equals(")")){
				if(!commandTokens.get(k).equals(",")){
					temp_values[cptr++][0] = commandTokens.get(k);
				}
				k++;
			}
			k++;
			if(!commandTokens.get(k).equalsIgnoreCase("values") ||
				!commandTokens.get(k+1).equals("(")){
					System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
					return;
			} else{
				k += 2;
				cptr = 0;
				while(!commandTokens.get(k).equals(")")){
					if(!commandTokens.get(k).equals(",")){
						temp_values[cptr++][1] = commandTokens.get(k);
					}
					k++;
				}
			}

		}else if(commandTokens.get(3).equalsIgnoreCase("values")){
			int k = 4;
			int vptr = 0;
			if(!commandTokens.get(k).equals("(")){
				System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
				return;
			} else{
				k++;
				while(!commandTokens.get(k).equals(")")){
					if(!commandTokens.get(k).equals(",")){
						temp_values[vptr][0] = table.columnNames.get(vptr);
						temp_values[vptr++][1] = commandTokens.get(k);
					}
					k++;
				}
			}
		}

		//create an array of values at appropriate positions
		for(String[] names: temp_values){
			if(names[0] == null || names[1] == null){
				continue;
			}
			int j = table.columnNames.indexOf(names[0]);
			if(j == -1){
				System.out.println("Column " + names[0] + " does not exist.");
				return;
			}
			values[j] = names[1];
		}

		//check if each value is nullable
		for(int flag = 0; flag < values.length; flag++){
			if(values[flag] == null && !table.isNullable.get(flag)){
				System.out.println(table.columnNames.get(flag) + " can not be NULL!");
				return;
			}
		}

		ArrayList<Object> rows = new ArrayList<>();
		for(int i = 0; i < values.length; i++){
			if(values[i] != null){
				Constants.DataTypes datatype = table.columnTypes.get(i);
				Object value = DataTools.parseStr(datatype, values[i]);
				rows.add(value);
			}else{
				rows.add(null);
			}
		}
		if(table.insert(rows)){
			System.out.println("1 row inserted successfully.");
		}else{
			System.out.println("Insertion failed.");
		}

	}
	
	//method to delete a record
	public static void parseDelete(ArrayList<String> commandTokens) throws IOException{
		String columnName;
		Object value;
		String operator;

		if(!commandTokens.get(0).equalsIgnoreCase("delete") || !commandTokens.get(1).equalsIgnoreCase("from")){
			System.out.println("Command is invalid");
			return;
		}

		String tableName = commandTokens.get(2).toLowerCase();
		if(!Table.isTableExist(tableName)){
			System.out.println("Table " + tableName + " does not exist.");
			return;
		}

		Table table = new Table(tableName, true);
		int query_size = commandTokens.size();

		if(query_size > 3){
			if(!commandTokens.get(3).equalsIgnoreCase("where")){
				System.out.println("Command is invalid");
				return;
			}

			if(query_size != 7 && query_size != 8){
				System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
				return;
			}

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
			columnName = null;
			value = null;
			operator = null;
		}

		int deletedRows = table.delete(columnName, value, operator);
		if(deletedRows > 0){
			System.out.println(deletedRows + " rows are deleted!");
		}else{
			System.out.println("delete failed!");
		}
	}
	

	/**
	 *  Stub method for dropping tables
	 */
	public static void dropTable(ArrayList<String> commandTokens) throws IOException{
		if(!commandTokens.get(1).equalsIgnoreCase("table")){
			System.out.println("Command is Invalid");
			return;
		}


		Table table = new Table(commandTokens.get(2).toLowerCase(), true);
		if(table.dropTable()){
			System.out.println("Table " + table.tableName + " is dropped.");
		}else{
			System.out.println("Table " + table.tableName + " could not be dropped.");
		}
	}



	/**
	 *  Stub method for executing queries
	 */
	public static void parseQuery(ArrayList<String> commandTokens) throws IOException{
		boolean allColumns = false;
		String columnName;
		String value;
		String operator;
		ArrayList<String> columns = new ArrayList<>();
		String tableName;
		ArrayList<Record> records = new ArrayList<>();
		int query_size = commandTokens.size();
		if(query_size == 1){
			System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
			return;
		}

		int i = 1;
		if(commandTokens.get(i).equals("*")){
			allColumns = true;
			i++;
		}else{
			while(i < query_size && !(commandTokens.get(i).equalsIgnoreCase("from"))){
				if(!(commandTokens.get(i).equalsIgnoreCase((",")))){
					columns.add(commandTokens.get(i));
				}
				i++;
			}
			if(i == query_size){
				System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
				return;
			}
		}
		i++;
		if(i == query_size){
			System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
			return;
		}

		tableName = commandTokens.get(i).toLowerCase();
		if(!Table.isTableExist(tableName)){
			System.out.println("Table does not exist");
			return;
		}
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

		i++;
		if(query_size == i){
			try{
				records = table.searchTable(null, null, null);
			} catch(Exception e){
				throw new RuntimeException(e);
			}
		}else if (commandTokens.get(i).equalsIgnoreCase("where")){
			i++;
			if(i + 3 == query_size || i + 4 == query_size){
				if(commandTokens.get(i).equalsIgnoreCase("not")){
					columnName = commandTokens.get(i+1);
					value = commandTokens.get(i+3);
					operator = inverseOperator(commandTokens.get(i+2));
					if(operator == null){
						System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
						return;
					}
				} else{
					columnName = commandTokens.get(i);
					operator = commandTokens.get(i+1);
					value = commandTokens.get(i+2);
				}

				Constants.DataTypes datatype = table.getColumnType(columnName);
				Object valueObject = DataTools.parseStr(datatype, value);
				records = table.searchTable(columnName.toLowerCase(), valueObject, operator);
			}else{
				System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
				return;
			}
		}

		Commands.displayRecords(table, records, columns, allColumns);
	}

	/**
	 *  Stub method for updating records
	 *  @param updateString is a String of the user input
	 */
	public static void parseUpdate(ArrayList<String> commandTokens) throws IOException {
		String columnName;
		Object value;
		String operator;
		String updateColumn;
		Object updateValue;

		if(!commandTokens.get(0).equalsIgnoreCase("update") || !commandTokens.get(2).equalsIgnoreCase("set")){
			System.out.println("Invalid Command Syntax");
			return;
		}

		String tableName = commandTokens.get(1).toLowerCase();
		if(!Table.isTableExist(tableName)){
			System.out.println("Table " + tableName + " does not exist.");
			return;
		}

		Table table = new Table(commandTokens.get(1).toLowerCase(), true);
		updateColumn = commandTokens.get(3);
		Constants.DataTypes updateColumnType = table.getColumnType(updateColumn);
		updateValue = DataTools.parseStr(updateColumnType, commandTokens.get(5));

		int query_size = commandTokens.size();
		if(query_size > 6){
			if(!commandTokens.get(6).equalsIgnoreCase("where") && query_size != 11 && query_size != 10){
				System.out.println("Invalid Command Syntax");
				return;
			}

			if(commandTokens.get(7).equalsIgnoreCase(("not"))){
				columnName = commandTokens.get(8).toLowerCase();
				Constants.DataTypes datatype = table.getColumnType(columnName);
				value = DataTools.parseStr(datatype, commandTokens.get(10));
				operator = inverseOperator(commandTokens.get(9));
				if(operator == null){
					System.out.println("Invalid operator");
					return;
				}
			}else{
				columnName = commandTokens.get(7).toLowerCase();
				Constants.DataTypes datatype = table.getColumnType(columnName);
				operator = commandTokens.get(8);
				value = DataTools.parseStr(datatype, commandTokens.get(9));
			}
		}else{
			columnName = null;
			operator = null;
			value = null;
		}
		int updatedRowCount = table.update(columnName, value, operator, updateColumn, updateValue);
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

	public static void displayRecords(Table table, ArrayList<Record> data, ArrayList<String> selectedColumns, boolean allColumns){
		ArrayList<Integer> columnNums = new ArrayList<>();
		ArrayList<Integer> columnSizes = new ArrayList<>();
		if(allColumns){
			for(int i = 0; i < table.columnNames.size(); i++){
				columnNums.add(i);
			}
		}else{
			for(String column: selectedColumns){
				columnNums.add(table.columnNames.indexOf(column.toLowerCase()));
			}
		}
		Collections.sort(columnNums);

		for(Integer i: columnNums){
			int maxColLength = table.columnNames.get(i).length();
			Constants.DataTypes dataType = table.getColumnType(table.columnNames.get(i));
			switch(dataType){
				case YEAR:
					maxColLength = Math.max(4, maxColLength);
					break;
				case TIME:
					maxColLength = Math.max(8, maxColLength);
					break;
				case DATETIME:
					maxColLength = Math.max(19, maxColLength);
					break;
				case DATE:
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

		//compute column separation length
		int colSeparation = (columnNums.size() - 1) * 3 + 4;

		for(Integer integer: columnSizes){
			colSeparation += integer;
		}

		//print line
		System.out.println(Utils.printSeparator("-", colSeparation));

		StringBuilder nameBuilder = new StringBuilder("|");
		for(Integer i: columnNums){
			nameBuilder.append(" ").append(String.format("%-" + columnSizes.get(i) + "s", table.columnNames.get(i))).append(" |");
		}
		System.out.println(nameBuilder);

		//print line
		System.out.println(Utils.printSeparator("-", colSeparation));

		//print data
		for(Record record: data){
			nameBuilder = new StringBuilder("|");
			for(Integer col: columnNums){
				Constants.DataTypes datatype = table.getColumnType(table.columnNames.get(col));
				Object val = record.getValues().get(col);
				String dataStr = DataTools.toStr(datatype, val);
				nameBuilder.append(" ").append(String.format("%-" + columnSizes.get(col) + "s", dataStr)).append(" |");

			}
			System.out.println(nameBuilder);
		}

		//print line
		System.out.println(Utils.printSeparator("-", colSeparation));

	}

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
		System.out.println("SELECT âŸ¨column_listâŸ© FROM table_name [WHERE condition];\n");
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
