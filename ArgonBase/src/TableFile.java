import java.util.*;
import java.io.*;


/*
 * Represents a Table File in the Argon database
 * Extends DatabaseFile class and handles indexing for a specific column of a table
 */
public class TableFile extends DatabaseFile{

    //constructor to create table file instance 
    public TableFile(String tableName, String path) throws IOException {
        super(tableName + ".tbl", Constants.PageType.TABLE_LEAF, path);
    }


    //retrieves the smallest row ID from the first record on the specified page
    public int getSmallestRowId(int page) throws IOException {
        //check if the page is empty
        if (getCellCount(page) <= 0){
            throw new IOException("Page is empty");
        }
        //determine the page type to appropriately skip bytes during data retrieval
        Constants.PageType pageType = getPageType(page);

         //get the offset of the first record on the page
        int offset = getCellOffset(page, 0);

        //move the file pointer to the specified offset
        this.seek((long) page * pageSize + offset);

        //skip bytes based on the page type to reach the row ID data
        if (pageType == Constants.PageType.TABLE_LEAF) {
            this.skipBytes(2);
        } else if (pageType == Constants.PageType.TABLE_INTERIOR) {
            this.skipBytes(4);
        }

        //read and return the smallest row ID from the first record on the page
        return this.readInt();
    }

    //retrieves the last row ID from the last leaf page of the table file
    public int getLastRowId() throws IOException {
        //get the page number of the last leaf page in the table file
        int lastPage = getLastLeafPage();
        //get the offset pointing to the start of content on the last leaf page
        int offset = getStartContent(lastPage);
        //get the count of cells on the last leaf page
        int numberOfCells = getCellCount(lastPage);
        //check if the last leaf page is empty and has no cells
        if (numberOfCells == 0){
            return -1;
        }
        //determine the page type to appropriately skip bytes during data retrieval
        Constants.PageType pageType = getPageType(lastPage);
        //move the file pointer to the specified offset
        this.seek((long) lastPage * pageSize + offset);
        //skip bytes based on the page type to reach the row ID data
        if (pageType == Constants.PageType.TABLE_LEAF) {
            this.skipBytes(2);
        } else if (pageType == Constants.PageType.TABLE_INTERIOR) {
            this.skipBytes(4);
        }
        //read and return the last row ID from the last leaf page
        return this.readInt();
    }


    //split page into two pages to mimic b+1 tree at specified row ID
    //no records are moved and if the root page is split, new root page is created
    //handles page splits in both leaf and interior pages of the table file
    public int pageSplit(int pageNumber, int splittingRowId) throws IOException {
        //determine the type of the page to be split like leaf or interior
        Constants.PageType pageType = getPageType(pageNumber);
        //get the parent page of the current page
        int parentPage = getParentPage(pageNumber);

        //create a new parent page if the current page has no existing parent
        if (parentPage == 0xFFFFFFFF) {
            //create a new parent page of type Table_INTERIOR
            parentPage = createPage(0xFFFFFFFF, Constants.PageType.TABLE_INTERIOR);

            //update the parent page pointer in the current page's header
            this.seek((long) pageNumber * pageSize + 0x0A);
            this.writeInt(parentPage);

            //write the page pointer for the new parent page to the root
            writePagePtr(parentPage, pageNumber, getSmallestRowId(pageNumber));
        }

        //create a new page of the same type as the original page and link to the parent
        int newPage = createPage(parentPage, pageType);
        writePagePtr(parentPage, newPage, splittingRowId);

        //update the page pointers if the original page is a leaf apge
        if (pageType == Constants.PageType.TABLE_LEAF) {
            this.seek((long) pageNumber * pageSize + 0x06);
            this.writeInt(newPage);
        }

        //return the page number of the newly created page after the split
        return newPage;
    }

    //write page pointer to interior page in format [page num][row id]
    //if necessary, it performs a page split to accomodate the new data
    //responsible for maintaining the structure of interior pages in the table file.
    //It ensures that page pointers and corresponding row IDs are correctly inserted, taking care of
    //page splits when needed to maintain the B-tree structure.
    public void writePagePtr(int page, int pointer, int rowId) throws IOException {
        //determine the size of the cell to be written (page pointer + row ID)
        short cellSize = 8;

        //check if a page split is needed to accomodate the new data
        if (split(page, cellSize)) {
            //perform a page split, updating the page number to the newly created page 
            page = pageSplit(page, rowId);
        }

        //set the start content pointer and get the incremement cell count
        short contentStart = setStartContent(page, cellSize);
        int numCells = incrementCellCount(page);

        //write the page pointer to the rightmost child in the header
        this.seek((long) page * pageSize + 0x06);
        this.writeInt(pointer); 

        //write to the cell pointer array, updating the array with the start content pointer
        this.seek((long) page * pageSize + 0x0E + numCells * 2);
        this.writeShort(contentStart); 

        //move to the start content pointer and write the cell data (page pointer + row ID)
        this.seek((long) page * pageSize + contentStart);
        this.writeInt(pointer); 
        this.writeInt(rowId);
    }

     //writes a record to the specified page in the table file, does page splitting if necessary
    public void writeRecord(Record record, int page) throws IOException {
        //calculate the total cell size, including the record length and additional metadata
        short recordSize = record.getRecordLength();

        //6 bytes for additional metadata
        short cellSize = (short) (recordSize + 6);

        //check if splitting the page is required to accomodate the new cell
        if (split(page, cellSize)) {
            page = pageSplit(page, record.getRowId());
        }

        //set the starting content position and increment the cell count on the page
        short contentStart = this.setStartContent(page, cellSize);
        short numberOfCells = incrementCellCount(page);

        //seek to the appropriate location in the page to write the cell pointer
        this.seek((long) pageSize * page + 0x0E + 2 * numberOfCells);

         //write to cell pointer array
        this.writeShort(contentStart);

        //seek to the starting content position to write the cell data
        this.seek((long) pageSize * page + contentStart);

         //extract information from the Record object
        ArrayList<Constants.DataTypes> columns = record.getColumns();
        ArrayList<Object> values = record.getValues();

        //write the record length and the row ID to the cell
        this.writeShort(recordSize);
        this.writeInt(record.getRowId());

        //write the page header of the record
        byte[] header = record.getPageHeader();
        this.write(header);

        //write the individual column values based on their data types
        for (int i = 0; i < columns.size(); i++){
            writeData(columns.get(i), values.get(i));
        }
    }


    //writes data of the specified data type to the current position in the table file
    //serializes and stores data of different data types in the table file
    public void writeData(Constants.DataTypes type, Object value) throws IOException {
         //switch based on the data type to handle different serialization formats
        switch(type){
            case TINYINT:
                this.writeByte((byte) value);
                break;

            case YEAR:
                this.writeByte((byte) value);
                break;
            
            case SMALLINT:
                this.writeShort((short) value);
                break;
            
            case INT:
                this.writeInt((int) value);
                break;
            
            case TIME:
                this.writeInt((int) value);
                break;
            
            case BIGINT:
                this.writeLong((long) value);
                break;
            
            case DATE:
                this.writeLong((long) value);
                break;

            case DATETIME:
                this.writeLong((long) value);
                break;

            case FLOAT:
                this.writeFloat((float) value);
                break;
            
            case DOUBLE:
                this.writeDouble((double) value);
                break;
            
            case TEXT:
                this.writeBytes((String) value);
                break;
        }

    }

    //append a record to a page
    public void appendRecord(Record record) throws IOException {
        int page = getLastLeafPage();
        writeRecord(record, page);
    }

    //updates the specified record with a new value at the specified column index
    public void updateRecord(int rowId, int columnIndex, Object newValue) throws IOException {
        //find the record information on the page
        int[] pageAndIndex = findRecord(rowId);
        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];

        //check if the record exists
        if (exists == 0) {
            throw new IOException("Record does not exist");
        }

        //get the cell offset of the record on the page
        int offset = getCellOffset(page, index);

        //read the existing record from the specified page and offset
        Record record = readRecord(page, offset);

        //handle TEXT column type separately due to variable length
        if (record.getColumns().get(columnIndex) == Constants.DataTypes.TEXT) {
            int oldSize = ((String) record.getValues().get(columnIndex)).length();
            int newSize = ((String) newValue).length();

            //check if splitting is required to accomodate the new TEXT value
            if (split(page, (short) (newSize - oldSize))) {
                //split the page and update the page information
                pageSplit(page, rowId);
                int[] newPageAndIndex = findRecord(rowId);
                page = newPageAndIndex[0];
                index = newPageAndIndex[1];
            }

            //adjust the cell pointers after splitting
            this.cellShift(page, index -1, oldSize - newSize, 0);

            //update the cell offset after splitting
            offset = getCellOffset(page, index);
        }

        //update the values of the record with the new value at the specified column index
        ArrayList<Object> values = record.getValues();
        values.set(columnIndex, newValue);
        Record newRecord = new Record(record.getColumns(), values, record.getRowId());

        //write the updated record to the specified page and offset
        this.seek((long) page * pageSize + offset + 6);
        byte[] header = newRecord.getPageHeader();
        this.write(header);
        for (int i = 0; i < newRecord.getColumns().size(); i++){
            writeData(newRecord.getColumns().get(i), values.get(i));
        }
    }

    //deletes the record with the specified rowID from the table
    public void deleteRecord(int rowId) throws IOException {
        //find the record information on the page
        int[] pageAndIndex = findRecord(rowId);
        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];

        //check if the record exists
        if (exists == 0) {
            throw new IOException("Record does not exist");
        }

        //get the cell offset of the record on the page
        int offset = getCellOffset(page, index);

        //move to the position of the payload size in the page
        this.seek((long) page * pageSize + offset);

        //move to the position of the payload size in the page
        short payloadSize = this.readShort();

        //shift the cells to remove the space occupied by the deleted record
        this.cellShift(page, index - 1, -payloadSize - 6, -1);

        //update the page pointer if the deleted record was the leftmost record
        if (index == 0 && getParentPage(page) != 0xFFFFFFFF) {
            updatePagePtr(getParentPage(page), index, getSmallestRowId(page));
        }


        //decrement cell count of the page
        int numCells = getCellCount(page);
        this.seek((long) page * pageSize + 0x02);
        this.writeShort((short) (numCells - 1));


        //write 0x00 between end of cell pointer array and the start of the page content
        int contentStart = getStartContent(page);
        int cellPointerArrayEnd = 0x10 + 2 * numCells;
        this.seek((long) page * pageSize + cellPointerArrayEnd);
        for (int i = cellPointerArrayEnd; i < contentStart; i++) {
            this.writeByte(0x00);
        }
    }

    //updates the row ID of a page pointer at the specified index within a cell on a given page
    //additionally, if the updated page pointer corresponds tot he leftmost child on the page,
    //recursively updates the parent page pointer
    public void updatePagePtr(int page, int index, int newRowId) throws IOException {
        //get the offset of the cell containing the page pointer
        int offset = getCellOffset(page, index);
        
        //seek to the location of the existing row ID in the page pointer
        this.seek((long) page * pageSize + offset + 4);

        //read the old Row ID from the page pointer
        int oldRowId = this.readInt();

        //seek back to the location of the existing row ID to update it with the new Row ID
        this.seek((long) page * pageSize + offset + 4);
        this.writeInt(newRowId);

        //check if the updated page pointer corresponds to the leftmost child on the page
        if (index == 0) {
            //determine the parent page and index of the updated page pointer
            int parentPage = getParentPage(page);
            int parentIndex = findPageRecord(parentPage, oldRowId);

            //recursively update the parent page pointer with the new Row ID
            updatePagePtr(parentPage, parentIndex, newRowId);
        }
    }

    //retrieves the page number of the last leaf page in the table file
    //traverses the index pages of the table file, following the rightmost
    //child pointers until it reaches the leaf level, identifying the last leaf page
    public int getLastLeafPage() throws IOException {
        //start from the root page of the table file
        int nextPage = getRootPage();

        //continuously traverse index pages until a leaf page is encountered
        while (true) {
            //determine the type of the current page like index or leaf
            Constants.PageType pageType = getPageType(nextPage);

            //break the loop if the current page is a leaf page
            if (pageType == Constants.PageType.TABLE_LEAF) {
                break;
            }

            //move the file pointer to the rightmost child pointer on the current index page
            this.seek((long) pageSize * nextPage + 0x06);

            //update the nextPage variable with the rightmost child pointer
            nextPage = this.readInt();
        }

        //return the page number of the last leaf page
        return nextPage;
    }

    //retrieves the record with the specified rowID from the table
    public Record getRecord(int rowId) throws IOException {
        //find the record information on the page
        int[] pageAndIndex = findRecord(rowId);
        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];
        
         //check if the record exists
        if (exists == 0) {
            return null;
        }
        //get the cell offset of the record on the page
        int offset = getCellOffset(page, index);

        //read and return the record from the specified page and offset
        return readRecord(page, offset);
    }

    //performs a binary search to find the index of the cell on a specified page
    //that contains the given rowID or the closest one to it
    public int findPageRecord(int page, int rowId) throws IOException {
        //get the total number of cells on the page
        int numCells = getCellCount(page);

        //midpoint
        int currentCell = numCells / 2;
        
        //left
        int low = 0; 

         //right
        int high = numCells - 1;
        
        //get the Row ID of the current cell
        int currentRowId = getRowId(page, currentCell);

        //binary search over cells
        while (low < high) {
            //0x10 is location of cells in the page where each cell location is 2 bytes
            if (currentRowId < rowId) {
                //the target row ID might be inside the current cell or to the right
                low = currentCell; 
            } else if ( currentRowId > rowId) {
                //the target row ID is to the left and exclude the current cell
                high = currentCell - 1; 
            } else {
                //exact match found, break out of the loop
                break;
            }
            
            //update the current cell index for the next iteration
            currentCell = (low + high + 1) / 2;

            //get the row id of the current cell
            currentRowId = getRowId(page, currentCell);
        }

        //return the idnex of the cell containing the row ID or the closest one
        return currentCell;
    }


    //finds the location of a record with the given row ID within the table file
    //returns an integer array containing information about the record's location
    public int[] findRecord(int rowId) throws IOException{
        //index 0: the page number where the record is located
        //index 1: the index of the cell containing the record on the page
        //index 2: an boolean specifying whether the record was found (1) or not (0)


        //start the search from the root page of the table file
        int currentPage = getRootPage();

        while (true) {
            //determine the type of the current page (leaf or interior)
            Constants.PageType pageType = getPageType(currentPage);

            //find the index of the cell containing the record with the given row ID on the current page
            int currentCell = findPageRecord(currentPage, rowId);

            //get the actual row ID stored in the cell at the found index
            int currentRowId = getRowId(currentPage, currentCell);

            //check if the current page is a leaf page
            if (pageType == Constants.PageType.TABLE_LEAF) {
                //check if the found Row ID matches the target Row ID
                if (currentRowId == rowId) {
                    //record found on the leaf page
                    return new int[] {currentPage, currentCell, 1};
                } else {
                    //record not found on the leaf page
                    return new int[] {currentPage, currentCell, 0};
                }
            } else if (pageType == Constants.PageType.TABLE_INTERIOR) {
                //if the current page is an interior page, navigate to the child page indicated by the found cell
                int offset = getCellOffset(currentPage, currentCell);
                this.seek((long) currentPage * pageSize + offset);

                //move to the child page for further searching
                currentPage = this.readInt();
            }
        }
    }

    //retrieves the Row ID associated with a specific record on the given page and index
    private int getRowId(int page, int index) throws IOException {
        //determine the type of page like table leaf or table interior
        Constants.PageType pageType = getPageType(page);
        
        //page offset is the location of the row as number of bytes from beginning of page
        //calculate the offset of the record within the page
        int offset = getCellOffset(page, index);
        
        //set the file pointer to the beginning of the record
        this.seek((long) page * pageSize + offset);


        if(pageType == Constants.PageType.TABLE_INTERIOR) {
            //if it's a table interior page, skip page pointer (4 bytes)
            this.skipBytes(4); 
        } else {
            //if it's a table leaf page, skip record size (2 bytes)
            this.skipBytes(2); 
        }
        //read and return the row ID associated with the record
        return this.readInt();
    }

    //read a record from the specified page at the given offset 
    private Record readRecord(int pageNum, int pageOffset) throws IOException{
        this.seek((long) pageNum * pageSize + pageOffset);

        //read the record size (payload size excluding metadata)
        this.readShort();

        //read the row ID associated with the record
        int rowID = this.readInt();

        //read the number of columns in the record
        byte numColumns = this.readByte();

        //read an array of bytes representing the data types of each column
        byte[] colTypeBytes = new byte[numColumns];
        for(int i = 0; i < numColumns; i++){
            colTypeBytes[i] = this.readByte();
        }

        //initialize lists to store column values and data types
        ArrayList<Object> values = new ArrayList<>();
        ArrayList<Constants.DataTypes> columnTypes = new ArrayList<>();

        //iterate through the column types and read corresponding values
        for(byte colTypeByte: colTypeBytes){
            Constants.DataTypes dataType;

            //determine the data type based on the column type byte
            if(colTypeByte > 0x0C){
                dataType = Constants.DataTypes.TEXT;
            }else{
                dataType = Constants.DataTypes.values()[colTypeByte];
            }

            //add the data type to the list
            columnTypes.add(dataType);

            //read and add the value based on the determined data type
            switch(dataType){
                case TINYINT:
                    values.add(this.readByte());
                    break;
                case YEAR:
                    values.add(this.readByte());
                    break;
                case SMALLINT:
                    values.add(this.readShort());
                    break;
                case INT:
                    values.add(this.readInt());
                    break;
                case TIME:
                    values.add(this.readInt());
                    break;
                case BIGINT:
                    values.add(this.readLong());
                    break;
                case DATE:
                    values.add(this.readLong());
                    break;
                case DATETIME:
                    values.add(this.readLong());
                    break;
                case FLOAT:
                    values.add(this.readFloat());
                    break;
                case DOUBLE:
                    values.add(this.readDouble());
                    break;
                case TEXT:
                    int textLength = colTypeByte - 0x0C;
                    byte[] text = new byte[textLength];
                    this.readFully(text);
                    values.add(new String(text));
                    break;
                case NULL:
                    values.add(null);
                    break;
            }
        }

        //construct and return a Record object from the read data
        return new Record(columnTypes, values, rowID);
    }


    //searches for records in the table that satisfy a given condition specified by
    //the column index, comparison value, and comparison operator
    public ArrayList<Record> search(int columnIndex, Object value, String operator) throws IOException {
        //initialize an arraylist to store the records that satisfy the condition
        ArrayList<Record> records = new ArrayList<>();

        //get the page number of the root page
        int currentPage = getRootPage();
        Constants.PageType pageType = getPageType(currentPage);
        
        //traverse down to the first leaf page
        while (pageType != Constants.PageType.TABLE_LEAF) {
             //get the offset of the first cell in the current page
            int offset = getCellOffset(currentPage, 0);
            this.seek((long) currentPage * pageSize + offset);

            //read the page number of the leftmost child
            currentPage = this.readInt();
            pageType = getPageType(currentPage);
        }
        

        //iterate over all records in leaf pages
        while (currentPage != 0xFFFFFFFF) {
            this.seek((long) currentPage * pageSize);
            int numberOfCells = getCellCount(currentPage);
            
            //iterate over all records in current leaf page
            for (int i = 0; i < numberOfCells; i++) {
                //move to the position of the current cell in the cell pointer array
                this.seek((long) currentPage * pageSize + 0x10 + 2 * i);

                //get the offset of the current cell
                int currentOffset = getCellOffset(currentPage, i);

                //read the record from the current page and offset
                Record record = readRecord(currentPage, currentOffset);

                //check if the record satisfies the search condition
                if (record.compare(columnIndex, value, operator)) {
                    records.add(record);
                }
            }

            //move to the position of the page pointer in the page header
            this.seek((long) currentPage * pageSize + 0x06);

            //read the page number of the next leaf page
            currentPage = this.readInt();
        }
        //return the arraylist of records that satisfy the condition
        return records;
    }

}
