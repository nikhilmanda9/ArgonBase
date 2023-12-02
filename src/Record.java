import java.util.*;

public class Record {
    private short recordLength;
    private final ArrayList<Constants.DataTypes> columns;
    private final ArrayList<Object> values;
    private final int rowID;
    private final byte[] pageHeader;

    public Record(ArrayList<Constants.DataTypes> columns, ArrayList<Object> values, int rowID){
        this.columns = columns;
        this.values = values;
        this.rowID = rowID;
        this.pageHeader = new byte[1 + (columns.size())];
        this.pageHeader[0] = (byte) columns.size();
        this.recordLength = (short) (1 + (columns.size()));
        for(int i = 0; i < columns.size(); i++){
           var column = columns.get(i);
           var value = values.get(i);
           if(value == null){
                column = Constants.DataTypes.NULL;
           }else{
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

           if(column != Constants.DataTypes.TEXT){
                pageHeader[i + 1] = (byte) column.ordinal();
           }else{
                pageHeader[i + 1] = (byte) (column.ordinal() + ((String) value).length());
           }
        }
    }

    public byte[] getPageHeader(){
        return pageHeader;
    }

    public ArrayList<Object> getValues(){
        return values;
    }

    public int getRowId(){
        return rowID;
    }
    
    public short getRecordLength(){
        return recordLength;
    }

    public ArrayList<Constants.DataTypes> getColumns(){
        return columns;
    }

    public String toStr(){
        StringBuilder sb = new StringBuilder();
        sb.append("Record length: ").append(recordLength).append("\n\t");
        sb.append("Row ID: ").append(rowID).append("\n\t");
        for(int i = 0; i < columns.size(); i++){
            sb.append(columns.get(i)).append(": ").append(values.get(i).toString()).append("\n\t");
        }

        return sb.toString();
    }

    public boolean equals(Object obj){
        if(!(obj instanceof Record)){
            return false;
        }

        Record rec = (Record) obj;

        if(this.columns.size() != rec.columns.size()){
            return false;
        }

        for(int i = 0; i < this.columns.size(); i++){
            if(this.columns.get(i) != rec.columns.get(i)){
                return false;
            }
            
            if(!this.values.get(i).equals(rec.values.get(i))){
                return false;
            }
        }

        return true;
    }

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
