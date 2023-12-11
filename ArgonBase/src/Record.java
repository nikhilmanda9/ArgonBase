import java.util.*;

/*
 * This class is used to represent a record in a database table
 */
public class Record {
    //size of the record in bytes
    private short recordSize;

    //list of data types for each column
    private final ArrayList<Constants.DataTypes> columns;

    //list of values for each column
    private final ArrayList<Object> values;

    //unique identifier for the record
    private final int rowId;

    //header information for the record
    private final byte[] header;


    //Constructor to create a Record instance with the given columns, values, and rowID
    public Record(ArrayList<Constants.DataTypes> columns, ArrayList<Object> values, int rowId) {
        this.columns = columns;
        this.values = values;
        this.rowId = rowId;
        this.header = new byte[1 + (columns.size())];
        this.header[0] = (byte) columns.size();
        this.recordSize = (short) (1 + (columns.size()));
        
        //Calculate record size and set header information
        for (int i = 0; i < columns.size(); i++) {
            var column = columns.get(i);
            var value = values.get(i);
            
            //If the value is null, set the column type to null
            if (value == null) {
                column = Constants.DataTypes.NULL;
            } else {
                //Calculate the size of the value
                int size = DataTools.typeSize(column);
                recordSize += size != -1 ? size : ((String) value).length();
            }

            //Set header information based on column type 
            if (column != Constants.DataTypes.TEXT) {
                header[i + 1] = (byte) column.ordinal();
            } else {
                header[i + 1] = (byte) (column.ordinal() + ((String) value).length());
            }
        }
    }

    //gets the size of the record in bytes
    public short getRecordLength() {
        return recordSize;
    }

    //gets the header information for the record and returns byte array representing the record
    public byte[] getPageHeader() {
        return header;
    }

    //gets the list of data types for each column and returns a list of data types
    public ArrayList<Constants.DataTypes> getColumns() {
        return columns;
    }

    //gets the list of values for each column and returns the list of values
    public ArrayList<Object> getValues() {
        return values;
    }

    //gets the unique identifier,rowID, for the record
    public int getRowId() {
        return rowId;
    }

    //returns a string representation of the record
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Record size: ").append(recordSize).append("\n\t");
        sb.append("Row id: ").append(rowId).append("\n\t");
        for (int i = 0; i < columns.size(); i++) {
            sb.append(columns.get(i)).append(": ").append(values.get(i).toString()).append("\n\t");
        }
        return sb.toString();
    }

    //compares the value of a specific column with a given value using the specified operator
    public boolean compare(int columnIndex, Object value, String operator) {
        if (columnIndex == -1 && value == null && operator == null) {
            return true;
        }
        Constants.DataTypes columnType = columns.get(columnIndex);
        if (columnType == Constants.DataTypes.NULL || value == null) {
            return false;
        }
        Object columnValue = values.get(columnIndex);
        return DataTools.compare(columnType, columnValue, value, operator);
    }


    //checks if this record is equal to another object
    public boolean equals(Object other) {
        //check if the other object is an instance of Record
        if (!(other instanceof Record)) {
            return false;
        }

        Record otherRecord = (Record) other;

        //check if the number of columns is the same
        if (this.columns.size() != otherRecord.columns.size()) {
            return false;
        }

        //compare each column and its corresponding value
        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i) != otherRecord.columns.get(i)) {
                return false;
            }
            if (!this.values.get(i).equals(otherRecord.values.get(i))) {
                return false;
            }
        }

        //return true when records are equal
        return true;
    }
}

