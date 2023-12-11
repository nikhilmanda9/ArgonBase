import java.util.*;
/*
 * This class is a helper class that provides useful data conversion methods and operations with data
 */
public class DataTools {

    //Given the value and its data type, convert it to its string representation
    public static String toStr(Constants.DataTypes dataType, Object value){
        
        //return null if the value is null
        if(value == null){
            return "NULL";
        }

        String str = "";

        //convert to appropriate database format
        switch(dataType){
            case YEAR:
                String date = Byte.toString((Byte) value);
                String yearStr = date.substring(0,4);
                int yearNum = Integer.parseInt(yearStr);
                str = String.valueOf(yearNum - 2000);
                break;

            case TIME:
                String time = Integer.toString((Integer) value);
                int milliseconds = Integer.parseInt(time.substring(0, 2)) * 3600000 *
                                   Integer.parseInt(time.substring(3,5)) * 60000 +
                                   Integer.parseInt(time.substring(6, 8)) * 1000;
                str = String.valueOf(milliseconds);
                break;

            case DATETIME:
                String dateTime = Long.toString((Long) value);

                Calendar cal = Calendar.getInstance();

                //set year
                cal.set(Calendar.YEAR, Integer.parseInt(dateTime.substring(0, 4)));

                //set month where month starts with 0 as January
                cal.set(Calendar.MONTH, Integer.parseInt(dateTime.substring(5,7)) - 1);

                //set date
                cal.set(Calendar.DATE, Integer.parseInt(dateTime.substring(8,10)));

                //set hour
                cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(dateTime.substring(11,13)));
                
                //set minutes
                cal.set(Calendar.MINUTE, Integer.parseInt(dateTime.substring(14,16)));

                //set seconds
                cal.set(Calendar.SECOND, Integer.parseInt(dateTime.substring(17,19)));

                Date d = cal.getTime();

                str = String.valueOf(d.getTime());
                
                break;



            case DATE:
                String dateStr = Long.toString((Long) value).substring(0, 10);

                Calendar calendar = Calendar.getInstance();

                //set year
                calendar.set(Calendar.YEAR, Integer.parseInt(dateStr.substring(0, 4)));

                //set month where month starts with 0 as January
                calendar.set(Calendar.MONTH, Integer.parseInt(dateStr.substring(5,7)) - 1);

                //set date
                calendar.set(Calendar.DATE, Integer.parseInt(dateStr.substring(8,10)));

                //set hour
                calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(dateStr.substring(11,13)));
                
                //set minutes
                calendar.set(Calendar.MINUTE, Integer.parseInt(dateStr.substring(14,16)));

                //set seconds
                calendar.set(Calendar.SECOND, Integer.parseInt(dateStr.substring(17,19)));

                Date dat = calendar.getTime();

                str = String.valueOf(dat.getTime());
                
                break;

            case NULL:
                str = "NULL";
                break;

            default:
                str = value.toString();
                break;
                
        }

        return str.trim();
    }

    //Takes a string and data type and returns the parsed string to its corresponding data type
    public static Object parseStr(Constants.DataTypes dataType, String str){
        switch(dataType){
            case TINYINT:
                return Byte.parseByte(str);
            
            case SMALLINT:
                return Short.parseShort(str);
            
            case INT:
                return Integer.parseInt(str);
            
            case BIGINT:
                return Long.parseLong(str);
            
            case FLOAT:
                return Float.parseFloat(str);
            
            case DOUBLE:
                return Double.parseDouble(str);
            
            case YEAR:
                String yearStr = str.substring(0,4);
                int yearNum = Integer.parseInt(yearStr);

                return Byte.parseByte(String.valueOf(yearNum - 2000));

            case TIME:
                int milliseconds = Integer.parseInt(str.substring(0, 2)) * 3600000 *
                                    Integer.parseInt(str.substring(3,5)) * 60000 +
                                    Integer.parseInt(str.substring(6, 8)) * 1000;
                str = String.valueOf(milliseconds);

                return Integer.parseInt(str);

            case DATETIME:
                Calendar c1 = Calendar.getInstance();

                //set year
                c1.set(Calendar.YEAR, Integer.parseInt(str.substring(0, 4)));

                //set month where month starts with 0 as January
                c1.set(Calendar.MONTH, Integer.parseInt(str.substring(5,7)) - 1);

                //set date
                c1.set(Calendar.DATE, Integer.parseInt(str.substring(8,10)));

                //set hour
                c1.set(Calendar.HOUR_OF_DAY, Integer.parseInt(str.substring(11,13)));
                
                //set minutes
                c1.set(Calendar.MINUTE, Integer.parseInt(str.substring(14,16)));

                //set seconds
                c1.set(Calendar.SECOND, Integer.parseInt(str.substring(17,19)));

                
                str = String.valueOf(c1.getTime().getTime());


                return Long.parseLong(str);

            case DATE:
                str = str + "_00:00:00";

                Calendar c2 = Calendar.getInstance();

                //set year
                c2.set(Calendar.YEAR, Integer.parseInt(str.substring(0, 4)));

                //set month where month starts with 0 as January
                c2.set(Calendar.MONTH, Integer.parseInt(str.substring(5,7)) - 1);

                //set date
                c2.set(Calendar.DATE, Integer.parseInt(str.substring(8,10)));

                //set hour
                c2.set(Calendar.HOUR_OF_DAY, Integer.parseInt(str.substring(11,13)));
                
                //set minutes
                c2.set(Calendar.MINUTE, Integer.parseInt(str.substring(14,16)));

                //set seconds
                c2.set(Calendar.SECOND, Integer.parseInt(str.substring(17,19)));

                
                str = String.valueOf(c2.getTime().getTime());


                return Long.parseLong(str);

            case TEXT:
                return str;
            
            default:
                return null;
        }
    }

    //compares two values of a specific datatype based on the specified operator
    public static boolean compareWithOp(Constants.DataTypes columnType, Object value1, Object value2, String operator){
        
        //if either value1 or value2 are null, return false
        if(value1 == null || value2 == null){
            return false;
        }

        //stores the result of the comparison
        int comparison = compareCol(columnType, value1, value2);

        switch(operator){
            case ">":
                return comparison > 0;
            
            case ">=":
                return comparison >= 0;
            
            case "<":
                return comparison < 0;
            
            case "<=":
                return comparison <= 0;
            
            case "=":
                return comparison == 0;
            
            case "<>":
                return comparison != 0;
            
            default:
                return false;
        }
    }

    //compare two values of a specific data type
    public static int compareCol(Constants.DataTypes columnType, Object value1, Object value2){
        
        //if either value1 or value2 are null, return 0
        if(value1 == null || value2 == null){
            return 0;
        }

        switch(columnType){
            case TINYINT:
                return compareVal((byte) value1, (byte) value2);
            
            case YEAR:
                return compareVal((byte) value1, (byte) value2);
            
            case SMALLINT:
                return compareVal((short) value1, (short) value2);

            case INT:
                return compareVal((int) value1, (int) value2);
            
            case TIME:
                return compareVal((int) value1, (int) value2);
            
            case BIGINT:
                return compareVal((long) value1, (long) value2);
            
            case DATETIME:
                return compareVal((long) value1, (long) value2);
            
            case DATE:
                return compareVal((long) value1, (long) value2);
            
            case FLOAT:
                return compareVal((float) value1, (float) value2);
            
            case DOUBLE:
                return compareVal((double) value1, (double) value2);
            
            case TEXT:
                return compareVal((String) value1, (String) value2);
            
            default:
                return 0;

        }
    }

    //helper method to compare two Comparable values
    public static <T extends Comparable<T>> int compareVal(T value1, T value2){
        
        //if either value1 or value2 are null, return 1
        if(value1 == null || value2 == null){
            return 1;
        }

        return value1.compareTo(value2);
    }
}