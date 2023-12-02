public class Constants {
    //Page size for all files is 512 bytes by default.
    public static final int PAGE_SIZE = 512;

    public enum PageType{
        INDEX_INTERIOR(0x02), INDEX_LEAF(0x0D), TABLE_INTERIOR(0x05), 
        TABLE_LEAF(0x0A), EMPTY(0x00), INVALID(0xFF);

        private final int cell_value; 

        PageType(int cell_value){
            this.cell_value = cell_value;
        }

        public int getValue(){
            return cell_value;
        }

        public static PageType fromValue(int cell_value){
            for (PageType pageType : PageType.values()){
                if(pageType.getValue() == cell_value){
                    return pageType;
                }
            }

            return INVALID;
        }
    }

    public enum FileType{
        TABLE, INDEX
    }

    public enum DataTypes{
        NULL, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, YEAR, TIME, DATETIME, DATE, TEXT, UNUSED
    }
}
