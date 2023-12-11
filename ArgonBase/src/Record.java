import java.util.*;

/*
 * This class is used to represent a record in a database table
 */
public class Record {
    //size of the record in bytes
    private short recordLength;

    //list of data types for each column
    private final ArrayList<Constants.DataTypes> columns;

    //list of values for each column
    private final ArrayList<Object> values;

    //unique identifier for the record
    private final int rowID;

    //header information for the record
    private final byte[] pageHeader;



    //Constructor to create a Record instance with the given columns, values, and rowID
    public Record(ArrayList<Constants.DataTypes> columns, ArrayList<Object> values, int rowID){
        this.columns = columns;
        this.values = values;
        this.rowID = rowID;
        this.pageHeader = new byte[1 + (columns.size())];
        this.pageHeader[0] = (byte) columns.size();
        this.recordLength = (short) (1 + (columns.size()));

        //Calculate record size and set header information
        for(int i = 0; i < columns.size(); i++){
           var column = columns.get(i);
           var value = values.get(i);

           //If the value is null, set the column type to null
           if(value == null){
                column = Constants.DataTypes.NULL;
           }else{
                //Calculate the size of the value
                Constants.DataTypes datatype = column;
                int size;
                switch(datatype){
                    case TINYINT:
                        size = 1;
                        break;

                    case YEAR:
                        size = 1;
                        break;
                    
                    case SMALLINT:
                        size = 2;
                        break;
                    
                    case INT:
                        size = 4;
                        break;
                    
                    case TIME:
                        size = 4;
                        break;
                    
                    case FLOAT:
                        size = 4;
                        break;
                    
                    case BIGINT:
                        size = 8;
                        break;
                    
                    case DATETIME:
                        size = 8;
                        break;
                    
                    case DATE:
                        size = 8;
                        break;
                    
                    case DOUBLE:
                        size = 8;
                        break;
                    
                    case TEXT:
                        size = -1;
                        break;
                    
                    default:
                        size = 0;
                        break;
                }

                if (size != -1) {
                    recordLength += size;
                } else {
                    recordLength += ((String) value).length();
                }  

           }

           //Set header information based on column type
           if(column != Constants.DataTypes.TEXT){
                pageHeader[i + 1] = (byte) column.ordinal();
           }else{
                pageHeader[i + 1] = (byte) (column.ordinal() + ((String) value).length());
           }
        }
    }

    //gets the header information for the record and returns byte array representing the record
    public byte[] getPageHeader(){
        return pageHeader;
    }

    //gets the list of values for each column and returns the list of values
    public ArrayList<Object> getValues(){
        return values;
    }

    //gets the unique identifier,rowID, for the record
    public int getRowId(){
        return rowID;
    }
    
    //gets the size of the record in bytes
    public short getRecordLength(){
        return recordLength;
    }

    //gets the list of data types for each column and returns a list of data types
    public ArrayList<Constants.DataTypes> getColumns(){
        return columns;
    }

    //returns a string representation of the record
    public String toStr(){
        StringBuilder sb = new StringBuilder();
        sb.append("Record length: ").append(recordLength).append("\n\t");
        sb.append("Row ID: ").append(rowID).append("\n\t");
        for(int i = 0; i < columns.size(); i++){
            sb.append(columns.get(i)).append(": ").append(values.get(i).toString()).append("\n\t");
        }

        return sb.toString();
    }

    //checks if this record is equal to another object
    public boolean equals(Object obj){
        //check if the other object is an instance of Record
        if(!(obj instanceof Record)){
            return false;
        }

        Record rec = (Record) obj;

        //check if the number of columns is the same
        if(this.columns.size() != rec.columns.size()){
            return false;
        }

        //compare each column and its corresponding value
        for(int i = 0; i < this.columns.size(); i++){
            if(this.columns.get(i) != rec.columns.get(i)){
                return false;
            }
            
            if(!this.values.get(i).equals(rec.values.get(i))){
                return false;
            }
        }

        return true; //return true when records are equal
    }

    //compares the value of a specific column with a given value using the specified operator
    public boolean compare(int columnIndex, Object value, String operator){
        if(columnIndex == -1 && value == null && operator == null){
            return true;
        }

        Constants.DataTypes columnType = columns.get(columnIndex);
        if(columnType == Constants.DataTypes.NULL || value == null){
            return false;
        }

        Object columnVal = values.get(columnIndex);
        return DataTools.compareWithOp(columnType, columnVal, value, operator);
    }

}
