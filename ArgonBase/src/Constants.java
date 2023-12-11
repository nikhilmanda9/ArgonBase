public class Constants {
    
    //Page Size for all files is 512 bytes by default
    public static final int PAGE_SIZE = 512;

    //Enum representing different types of pages in the database file
    public enum PageType {
        //Page type for index interior pages initialized with hexadecimal 0x02
        INDEX_INTERIOR(0x02),
        //page type for table interior pages initialized with hexadecimal Ox05 
        TABLE_INTERIOR(0x05),
        //page type for table leaf pages initialized with hexadecimal 0x0A 
        TABLE_LEAF(0x0A),
        //page type for index leaf pages initialized with hexadecimal 0x0D 
        INDEX_LEAF(0x0D),
        //page type for empty pages initialized with hexadecimal 0x00 
        EMPTY(0x00),
        //page type for invalid pages initialized with hexadecimal 0xF 
        INVALID(0xFF);

        private final int cellValue;

        //constructor
        PageType(int value) { 
            this.cellValue = value; 
        }

        //Gets the cell value associated with the page type
        public int getValue() { 
            return cellValue; 
        }
        
        //Given the cell value, it converts it to the corresponding page type
        public static PageType fromValue(int cellValue) {
            for (PageType pageType : PageType.values()) {
                if (pageType.getValue() == cellValue) {
                    return pageType;
                }
            }
            return INVALID;
        }
    }

    //Enum representing different types of files in the database
    public enum FileType { 
        TABLE, INDEX 
    }

    //Enum representing different data types in the database
    public enum DataTypes {
        NULL, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, UNUSED, YEAR, TIME, DATETIME, DATE, TEXT
    }

}
