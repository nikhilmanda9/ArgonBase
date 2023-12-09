import java.io.*;
import java.util.*;

/*
 * Represents a Table File in the Argon database
 * Extends DatabaseFile class and handles indexing for a specific column of a table
 */
public class TableFile extends DatabaseFile{
    
    //constructor to create table file instance 
    public TableFile(String tableName, String path) throws IOException{
        super(tableName + ".tbl", Constants.PageType.TABLE_LEAF, path);
    }

    //retrieves the smallest row ID from the first record on the specified page
    public int getSmallestRowID(int pageNum) throws IOException{
        //check if the page is empty
        if(getCellCount(pageNum) <= 0){
            throw new IOException("Empty page");
        }

        //determine the page type to appropriately skip bytes during data retrieval
        Constants.PageType pageType = getPageType(pageNum);

        //get the offset of the first record on the page
        int offset = getCellOffset(pageNum, 0);

        //move the file pointer to the specified offset
        this.seek((long) pageNum * pageSize + offset);

        //skip bytes based on the page type to reach the row ID data
        if(pageType == Constants.PageType.TABLE_LEAF){
            this.skipBytes(2);
        } else if(pageType == Constants.PageType.TABLE_INTERIOR){
            this.skipBytes(4);
        }

        //read and return the smallest row ID from the first record on the page
        return this.readInt();
    }

    //retrieves the last row ID from the last leaf page of the table file
    public int getLastRowID() throws IOException{
        //get the page number of the last leaf page in the table file
        int lastPage = getLastLeafPage();

        //get the offset pointing to the start of content on the last leaf page
        int offset = getStartContent(lastPage);

        //get the count of cells on the last leaf page
        int cellCount = getCellCount(lastPage);

        //check if the last leaf page is empty and has no cells
        if(cellCount == 0){
            return -1;
        }

        //determine the page type to appropriately skip bytes during data retrieval
        Constants.PageType pageType = getPageType(lastPage);

        //move the file pointer to the specified offset
        this.seek((long) lastPage * pageSize + offset);

        //skip bytes based on the page type to reach the row ID data
        if(pageType == Constants.PageType.TABLE_LEAF){
            this.skipBytes(2);
        }else if(pageType == Constants.PageType.TABLE_INTERIOR){
            this.skipBytes(4);
        }

        //read and return the last row ID from the last leaf page
        return this.readInt();
    }

    //retrieves the page number of the last leaf page in the table file
    //traverses the index pages of the table file, following the rightmost
    //child pointers until it reaches the leaf level, identifying the last leaf page
    public int getLastLeafPage() throws IOException{
        //start from the root page of the table file
        int nextPage = getRootPage();

        //continuosly traverse index pages until a leaf page is encountered
        while(true){
            //determine the type of the current page like index or leaf
            Constants.PageType pageType = getPageType(nextPage);

            //break the loop if the current page is a leaf page
            if(pageType == Constants.PageType.TABLE_LEAF){
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

    //split page into two pages to mimic b+1 tree at specified row ID
    //no records are moved and if the root page is split, new root page is created
    //handles page splits in both leaf and interior pages of the table file
    public int pageSplit(int pageNum, int splitRowID) throws IOException{
        //determine the type of the page to be split like leaf or interior
        Constants.PageType pageType = getPageType(pageNum);
        //get the parent page of the current page
        int parentPage = getParentPage(pageNum);

        //create a new parent page if the current page has no existing parent
        if(parentPage == 0xFFFFFFFF){
            //create a new parent page of type Table_INTERIOR
            parentPage = createPage(0xFFFFFFFF, Constants.PageType.TABLE_INTERIOR);
            
            //update the parent page pointer in the current page's header
            this.seek((long) pageNum * pageSize + 0x0A);
            this.writeInt(parentPage);

            //write the page pointer for the new parent page to the root
            writePagePtr(parentPage, pageNum, getSmallestRowID(pageNum));    
        }

        //create a new page of the same type as the original page and link to the parent
        int newPage = createPage(parentPage, pageType);
        writePagePtr(parentPage, newPage, splitRowID);

        //update the page pointers if the original page is a leaf apge
        if(pageType == Constants.PageType.TABLE_LEAF){
            this.seek((long) pageNum * pageSize + 0x06);
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
    public void writePagePtr(int pageNum, int pagePtr, int rowID) throws IOException{
        //determine the size of the cell to be written (page pointer + row ID)
        short cellSize = 8;

        //check if a page split is needed to accomodate the new data
        if(split(pageNum, cellSize)){
            //perform a page split, updating the page number to the newly created page 
            pageNum = pageSplit(pageNum, rowID);
        }

        //set the start content pointer and get the incremement cell count
        short startContent = setStartContent(pageNum, cellSize);
        int cellCount = incrementCellCount(pageNum);

        //write the page pointer to the rightmost child in the header
        this.seek((long) pageNum * pageSize + 0x06);
        this.writeInt(pagePtr);

        //write to the cell pointer array, updating the array with the start content pointer
        this.seek((long) pageNum * pageSize + 0x0E + cellCount * 2);
        this.writeShort(startContent);

        //move to the start content pointer and write the cell data (page pointer + row ID)
        this.seek((long) pageNum * pageSize + startContent);
        this.writeInt(pagePtr);
        this.writeInt(rowID);
    }

    //writes data of the specified data type to the current position in the table file
    //serializes and stores data of different data types in the table file
    public void writeData(Constants.DataTypes datatype, Object obj) throws IOException{
        //switch based on the data type to handle different serialization formats
        switch(datatype){
            case TINYINT:
                this.writeByte((byte) obj);
                break;

            case YEAR:
                this.writeByte((byte) obj);
                break;
            
            case SMALLINT:
                this.writeShort((short) obj);
                break;
            
            case INT:
                this.writeInt((int) obj);
                break;
            
            case TIME:
                this.writeInt((int) obj);
                break;
            
            case BIGINT:
                this.writeLong((long) obj);
                break;
            
            case DATE:
                this.writeLong((long) obj);
                break;

            case DATETIME:
                this.writeLong((long) obj);
                break;

            case FLOAT:
                this.writeFloat((float) obj);
                break;
            
            case DOUBLE:
                this.writeDouble((double) obj);
                break;
            
            case TEXT:
                this.writeBytes((String) obj);
                break;
        }

    }

    //retrieves the Row ID associated with a specific record on the given page and index
    private int getRowID(int page, int pageIndex) throws IOException{
        //determine the type of page like table leaf or table interior
        Constants.PageType pageType = getPageType(page);

        //page offset is the location of the row as number of bytes from beginning of page
        //calculate the offset of the record within the page
        int pageOffset = getCellOffset(page, pageIndex);

        //set the file pointer to the beginning of the record
        this.seek((long) page * pageSize + pageOffset);

        if(pageType == Constants.PageType.TABLE_INTERIOR){
            //if it's a table interior page, skip page pointer (4 bytes)
            this.skipBytes(4);
        }else{
            //if it's a table leaf page, skip record size (2 bytes)
            this.skipBytes(2);
        }
        //read and return the row ID associated with the record
        return this.readInt();
    }


    //performs a binary search to find the index of the cell on a specified page
    //that contains the given rowID or the closest one to it
    public int findPageRecord(int page, int targetRowID) throws IOException{
        //get the total number of cells on the page
        int cellCount = getCellCount(page);

        //midpoint
        int curCell = cellCount / 2;

        //left
        int low = 0;

        //right
        int high = cellCount - 1;

        //get the Row ID of the current cell
        int curRowID = getRowID(page, curCell);

        //binary search over cells
        while(low < high){
            //0x10 is location of cells in the page where each cell location is 2 bytes
            if(curRowID < targetRowID){
                //the target row ID might be inside the current cell or to the right
                low = curCell;
            } else if(curRowID > targetRowID){
                //the target row ID is to the left and exclude the current cell
                high = curCell - 1;
            } else{
                //exact match found, break out of the loop
                break;
            }

            //update the current cell index for the next iteration
            curCell = (low + high + 1) / 2;

            //get the row id of the current cell
            curRowID = getRowID(page, curCell);
        }

        //return the idnex of the cell containing the row ID or the closest one
        return curCell;
    }

    //writes a record to the specified page in the table file, does page splitting if necessary
    public void writeRecord(Record rec, int page) throws IOException{
        //calculate the total cell size, including the record length and additional metadata
        short recordLength = rec.getRecordLength();

        //6 bytes for additional metadata
        short cellSize = (short) (recordLength + 6);

        //check if splitting the page is required to accomodate the new cell
        if(split(page, cellSize)){
            page = pageSplit(page, rec.getRowId());
        }

        //set the starting content position and increment the cell count on the page
        short startContent = this.setStartContent(page, cellSize);
        short cellCount = incrementCellCount(page);

        //seek to the appropriate location in the page to write the cell pointer
        this.seek((long) pageSize * page + 0x0E + 2 * cellCount);

        //write to cell pointer array
        this.writeShort(startContent);

        //seek to the starting content position to write the cell data
        this.seek((long) pageSize * page + startContent);

        //extract information from the Record object
        ArrayList<Constants.DataTypes> columns = rec.getColumns();
        ArrayList<Object> values = rec.getValues();

        //write the record length and the row ID to the cell
        this.writeShort(recordLength);
        this.writeInt(rec.getRowId());

        //write the page header of the record
        byte[] pageHeader = rec.getPageHeader();
        this.write(pageHeader);

        //write the individual column values based on their data types
        for(int i = 0; i < columns.size(); i++){
            writeData(columns.get(i), values.get(i));
        }

    }

    //updates the row ID of a page pointer at the specified index within a cell on a given page
    //additionally, if the updated page pointer corresponds tot he leftmost child on the page,
    //recursively updates the parent page pointer
    public void updatePagePtr(int page, int pageIndex, int newRowID) throws IOException{
        //get the offset of the cell containing the page pointer
        int cellOffset = getCellOffset(page, pageIndex);

        //seek to the location of the existing row ID in the page pointer
        this.seek((long) page * pageSize + cellOffset + 4);

        //read the old Row ID from the page pointer
        int oldRowID = this.readInt();

        //seek back to the location of the existing row ID to update it with the new Row ID
        this.seek((long) page * pageSize + cellOffset + 4);
        this.writeInt(newRowID);

        //check if the updated page pointer corresponds to the leftmost child on the page
        if (pageIndex == 0){
            //determine the parent page and index of the updated page pointer
            int parentPage = getParentPage(page);
            int parentIndex = findPageRecord(parentPage, oldRowID);

            //recursively update the parent page pointer with the new Row ID
            updatePagePtr(parentPage, parentIndex, newRowID);
        }

    }

    //finds the location of a record with the given row ID within the table file
    //returns an integer array containing information about the record's location
    public int[] findRecord(int targetRowID) throws IOException{
        //index 0: the page number where the record is located
        //index 1: the index of the cell containing the record on the page
        //index 2: an boolean specifying whether the record was found (1) or not (0)

        //start the search from the root page of the table file
        int curPage = getRootPage();

        while(true){
            //determine the type of the current page (leaf or interior)
            Constants.PageType pageType = getPageType(curPage);

            //find the index of the cell containing the record with the given row ID on the current page
            int curCell = findPageRecord(curPage, targetRowID);

            //get the actual row ID stored in the cell at the found index
            int curRowID = getRowID(curPage, curCell);

            //check if the current page is a leaf page
            if(pageType == Constants.PageType.TABLE_LEAF){
                //check if the found Row ID matches the target Row ID
                if(curRowID == targetRowID){
                    //record found on the leaf page
                    return new int[] {curPage, curCell, 1};
                }else{
                    //record not found on the leaf page
                    return new int[] {curPage, curCell, 0};
                }
            }else if(pageType == Constants.PageType.TABLE_INTERIOR){
                //if the current page is an interior page, navigate to the child page indicated by the found cell
                int pageOffset = getCellOffset(curPage, curCell);
                this.seek((long) curPage * pageSize + pageOffset);
                //move to the child page for further searching
                curPage = this.readInt();
            }
        }
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
    public ArrayList<Record> searchUsingCond(int columnIndex, Object compareVal, String compareOp) throws IOException{
        //initialize an arraylist to store the records that satisfy the condition
        ArrayList<Record> records = new ArrayList<>();

        //get the page number of the root page
        int curPage = getRootPage();
        Constants.PageType pageType = getPageType(curPage);

        //traverse down to the first leaf page
        while(pageType != Constants.PageType.TABLE_LEAF){
            //get the offset of the first cell in the current page
            int pageOffset = getCellOffset(curPage, 0);
            this.seek((long) curPage * pageSize + pageOffset);

            //read the page number of the leftmost child
            curPage = this.readInt();
            pageType = getPageType(curPage);
        }

        //iterate over all records in leaf pages
        while(curPage != 0xFFFFFFFF){
            this.seek((long) curPage * pageSize);
            int cellCount = getCellCount(curPage);

            //iterate over all records in current leaf page
            for(int i = 0; i < cellCount; i++){
                //move to the position of the current cell in the cell pointer array
                this.seek((long) curPage * pageSize + 0x10 + 2 * i);

                //get the offset of the current cell
                int curOffset = getCellOffset(curPage, i);

                //read the record from the current page and offset
                Record record = readRecord(curPage, curOffset);

                //check if the record satisfies the search condition
                if(record.compare(columnIndex, compareVal, compareOp)){
                    records.add(record);
                }
            }
            //move to the position of the page pointer in the page header
            this.seek((long) curPage * pageSize + 0x06);

            //read the page number of the next leaf page
            curPage = this.readInt();
        }
        //return the arraylist of records that satisfy the condition
        return records;
    }


    //updates the specified record with a new value at the specified column index
    public void updateRecord(int rowID, int columnIndex, Object newVal) throws IOException{
        //find the record information on the page
        int[] pageInfo = findRecord(rowID);
        int page = pageInfo[0];
        int pageIndex = pageInfo[1];
        int exists = pageInfo[2];

        //check if the record exists
        if(exists == 0){
            throw new IOException("Record doesn't exist");
        }

        //get the cell offset of the record on the page
        int pageOffset = getCellOffset(page, pageIndex);

        //read the existing record from the specified page and offset
        Record rec = readRecord(page, pageOffset);

        //handle TEXT column type separately due to variable length
        if(rec.getColumns().get(columnIndex) == Constants.DataTypes.TEXT){
            int oldSize = ((String) rec.getValues().get(columnIndex)).length();
            int newSize = ((String) newVal).length();

            //check if splitting is required to accomodate the new TEXT value
            if(split(page, (short) (newSize - oldSize))){
                //split the page and update the page information
                pageSplit(page, rowID);
                int[] newPageInfo = findRecord(rowID);
                page = newPageInfo[0];
                pageIndex = newPageInfo[1];
            }

            //adjust the cell pointers after splitting
            this.cellShift(page, pageIndex - 1, oldSize - newSize, 0);

            //update the cell offset after splitting
            pageOffset = getCellOffset(page, pageIndex);
        }

        //update the values of the record with the new value at the specified column index
        ArrayList<Object> values = rec.getValues();
        values.set(columnIndex, newVal);
        Record newRec = new Record(rec.getColumns(), values, rec.getRowId());

        //write the updated record to the specified page and offset
        this.seek((long) page * pageSize + pageOffset + 6);
        byte[] pageHeader = newRec.getPageHeader();
        this.write(pageHeader);
        for(int i = 0; i < newRec.getColumns().size(); i++){
            writeData(newRec.getColumns().get(i), values.get(i));
        }
    }

    //retrieves the record with the specified rowID from the table
    public Record getRecord(int rowID) throws IOException{
        //find the record information on the page
        int[] pageInfo = findRecord(rowID);
        int page = pageInfo[0];
        int index = pageInfo[1];
        int exists = pageInfo[2];

        //check if the record exists
        if(exists == 0){
            return null;
        }
        //get the cell offset of the record on the page
        int cellOffset = getCellOffset(page, index);

        //read and return the record from the specified page and offset
        return readRecord(page, cellOffset);
    }

    //deletes the record with the specified rowID from the table
    public void deleteRecord(int rowID) throws IOException{
        //find the record information on the page
        int[] pageInfo = findRecord(rowID);
        int page = pageInfo[0];
        int pageIndex = pageInfo[1];
        int exists = pageInfo[2];

        //check if the record exists
        if(exists == 0){
            throw new IOException("Record doesn't exist");
        }

        //get the cell offset of the record on the page
        int pageOffset = getCellOffset(page, pageIndex);

        //move to the position of the payload size in the page
        this.seek((long) page * pageSize + pageOffset);

        //move to the position of the payload size in the page
        short payloadSize = this.readShort();

        //shift the cells to remove the space occupied by the deleted record
        this.cellShift(page, pageIndex - 1, -payloadSize - 6, -1);

        //update the page pointer if the deleted record was the leftmost record
        if(pageIndex == 0 && getParentPage(page) != 0xFFFFFFFF){
            updatePagePtr(getParentPage(page), pageIndex, getSmallestRowID(page));
        }

        //decrement cell count of the page
        int cellCount = getCellCount(page);
        this.seek((long) page * pageSize + 0x02);
        this.writeShort((short) (cellCount - 1));

        //write 0x00 between end of cell pointer array and the start of the page content
        int startContent = getStartContent(page);
        int cellPtrArrEnd = 0x10 + 2 * cellCount;
        this.seek((long) page * pageSize + cellPtrArrEnd);
        for(int i = cellPtrArrEnd; i < startContent; i++){
            this.writeByte(0x00);
        }
    }

}