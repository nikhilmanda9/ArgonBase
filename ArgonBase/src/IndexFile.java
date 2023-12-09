import java.io.*;
import java.util.*;

/*
 * Represents an Index File in the Argon database
 * Extends DatabaseFile class and handles indexing for a specific column of a table
 */
public class IndexFile extends DatabaseFile{
    //datatype of the indexed column
    Constants.DataTypes dataType;
    //length of the indexed column value
    short valueLength;
    //name of the table associated with the index
    String tableName;
    //index of the column being indexed
    int columnIndex;
    //file path for the index file
    String path;

    //Creates a new IndexFile object
    public IndexFile(Table table, String columnName, String path) throws IOException{
        super(table.tableName + "." + columnName + ".ndx", Constants.PageType.INDEX_LEAF, path);
        this.tableName = table.tableName;
        this.columnIndex = table.columnNames.indexOf(columnName);
        this.path = path;
        this.dataType = table.getColumnType(columnName);
        //determine the size of the values based on data type
        switch(dataType){
            case TINYINT:
                this.valueLength = 1;
                break;
            
            case SMALLINT:
                this.valueLength = 2;
                break;
            
            case INT:
                this.valueLength = 4;
                break;
            
            case TIME:
                this.valueLength = 4;
                break;
            
            case FLOAT:
                this.valueLength = 4;
                break;
            
            case BIGINT:
                this.valueLength = 8;
                break;
            
            case DATE:
                this.valueLength = 8;
                break;
            
            case DATETIME:
                this.valueLength = 8;
                break;
            
            case DOUBLE:
                this.valueLength = 8;
                break;
            
            case TEXT:
                this.valueLength = -1;
                break;
            
            case NULL:
                this.valueLength = 0;
                break;
        }
    }

    //Reads the data from the specified page and offset in the index file
    public Object readData(int pageNum, int pageOffset) throws IOException{
        //determine the page type of the specified page
        Constants.PageType pageType = getPageType(pageNum);

        //set the file pointer to the specified location in the file based on the page number and offset
        this.seek((long) pageNum * pageSize + pageOffset);

        //if the page is index interior, skip 4 bytes which is possibly a page pointer
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }

        //read the payload size which is the length of the data
        int payLoadSize = this.readShort();

        //if the payload size is 0, return null which contains no valid data
        if(payLoadSize == 0){
            return null;

        }

        //skip 1 byte which additional information, possibly a record flag
        this.skipBytes(1);

        //read the record type
        byte recordType = this.readByte();

        //if the record type is 0, return null which is no valid data
        if(recordType == 0){
            return null;
        }
        
        //read the data based on the specified data type
        switch(dataType){
            case TINYINT:
                return this.readByte();
            case YEAR:
                return this.readByte();
            
            case SMALLINT:
                return this.readShort();

            case INT:
                return this.readInt();

            case TIME:
                return this.readInt();

            case BIGINT:
                return this.readLong();
            
            case DATE:
                return this.readLong();
            
            case DATETIME:
                return this.readLong();
            
            case FLOAT:
                return this.readFloat();

            case DOUBLE:
                return this.readDouble();
            
            case TEXT:
                //for TEXT, calculate text length and read the corresponding bytes
                int textLength = recordType - 0x0C;
                byte[] text = new byte[textLength];
                this.read(text);
                return new String(text);
            
            default:
                //return null for unsupported data types
                return null;
            
        }

    }
    
    //find the index of last cell on the page that has value <= given value
    public int findValIndex(Object targetVal, int pageNum) throws IOException{
        //get the total number of cells on the specified page
        int cellCount = getCellCount(pageNum);

        //if there are no cells on the page, return -1 because value is not found
        if(cellCount == 0){
            return -1;
        }

        //perform binary search

        //midpoint
        int mid = cellCount / 2;
        
        //left
        int low = -1;

        //right
        int high = cellCount - 1;

        //offset of the current cell
        int curOffset = getCellOffset(pageNum, mid);

        while(low < high){
            //read the value at the current offset
            Object curVal = readData(pageNum, curOffset);

            //compare the current value with the target value
            int comparison = DataTools.compareCol(dataType, curVal, targetVal);

            //if the current value is null, consider it smaller than the target value
            if(curVal == null){
                comparison = -1;
            }

            //check the result of the comparison
            if(comparison == 0){
                //target value found, return the current index
                return mid;
            } else if(comparison < 0){
                //current value is less than target value, adjust the left boundary
                low = mid;
            } else{
                //current value is greater than the target value, adjust the right boundary
                high = mid - 1;
            }

            //update the midpoint and current offset for the next iteration
            mid = (int) Math.floor((float) (low + high + 1) / 2f);
            curOffset = getCellOffset(pageNum, mid);
        }

        //if the loop completes without finding the value, return the last known midpoint 
        return mid;
    }

    //writes a cell to the specified page in the index file, containing the given value, associated row IDS,
    //and optional child page pointer for index interior pages
    public void writeCell(Object val, ArrayList<Integer> rowIDs, int page, int childPage) throws IOException{
        //determine the page type if it's index leaf or index interior
        Constants.PageType pageType = getPageType(page);

        //calculate payload and cell sizes based on the value type
        short payLoadSize = (short) (2 + valueLength + 4 * rowIDs.size());


        if(valueLength == -1){
            payLoadSize += ((String) val).length() + 1;
        }

        short cellSize = (short) (2 + payLoadSize);

        //adjust cell size for index interior pages
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            cellSize += 4;
        }

        //check if splitting is required and perform split if needed
        if(split(page, cellSize)){
            page = pageSplit(page, val);
        }

        //find the index where the cell should be inserted
        int insertPoint = findValIndex(val, page);

        int offset;
        //if the insert point is the last cell, set the offset to the start of the content
        if(insertPoint == getCellCount(page) - 1){
            offset = setStartContent(page, cellSize);
        } else{
            //otherwise, shift cells to accomodate the new cell and get the offset
            offset = cellShift(page, insertPoint, cellSize, 1);
        }

        //increment the cell count for the page
        incrementCellCount(page);

        //write cell start to cell pointer array
        this.seek((long) page * pageSize + 0x10 + 2L * (insertPoint + 1));
        this.writeShort(offset);

        //write number of records, data type, and optional child page pointer for index interior pages
        this.seek((long) page * pageSize + offset);
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            if(childPage == -1){
                throw new IOException("Child page not specified");
            }

            this.writeInt(childPage);
        }

        this.writeShort(payLoadSize);
        this.writeByte(rowIDs.size());

        //write data type
        if(dataType.ordinal() == 0x0C){
            this.writeByte(((String) val).length() + 0x0C);
        }else{
            this.writeByte(dataType.ordinal());
        }

        //write the actual value based on the data type
        switch(dataType){
            case TINYINT:
                this.writeByte((Byte) val);
                break;
            case YEAR:
                this.writeByte((Byte) val);
                break;
            case SMALLINT:
                this.writeShort((Short) val);
                break;
            case INT:
                this.writeInt((Integer) val);
                break;
            case TIME:
                this.writeInt((Integer) val);
                break;
            case BIGINT:
                this.writeLong((Long) val);
                break;
            case DATE:
                this.writeLong((Long) val);
                break;
            case DATETIME:
                this.writeLong((Long) val);
                break;
             case FLOAT:
                this.writeFloat((Float) val);
                break;
             case DOUBLE:
                this.writeDouble((Double) val);
                break;
             case TEXT:
                this.writeBytes((String) val);
                break;
        }

        //write row IDs associated with the value
        for(int rowId: rowIDs){
            this.writeInt(rowId);
        }

    }

    //move all cells after preceding cell on source page to destination page
    public void cellsToPage(int sourcePage, int destinationPage, int precedingCell) throws IOException{
        int cellOffset = getCellOffset(sourcePage, precedingCell);
        int cellCount = getCellCount(sourcePage);
        int cellsToMove = cellCount - precedingCell - 1;
        int startContent = getStartContent(sourcePage);

        //read the bytes to be moved
        byte[] cellBytes = new byte[cellOffset - startContent];
        this.seek((long) sourcePage * pageSize + startContent);
        this.read(cellBytes);

        //read the offsets of the cells to be moved
        byte[] cellOffsets = new byte[(getCellCount(sourcePage) - precedingCell - 1) * 2];
        this.seek((long) sourcePage * pageSize + 0x10 + (precedingCell + 1) * 2L);
        this.read(cellOffsets);

        //overwrite the old cell offsets with zeroes
        this.seek((long) sourcePage * pageSize + 0x10 + (precedingCell + 1) * 2L);
        byte[] zeroes = new byte[cellOffsets.length];
        this.write(zeroes);

        //write the bytes to be moved
        int newStartContent = getStartContent(destinationPage) - (cellBytes.length);
        this.seek((long) destinationPage * pageSize + newStartContent);
        this.write(cellBytes);

        //write the new content start pointer in the destination page
        this.seek((long) destinationPage * pageSize + 0x04);
        this.writeShort(newStartContent);

        //write the cell offsets that that need to be moved
        int offsetDiff = startContent - newStartContent;
        this.seek((long) destinationPage * pageSize + 0x10);
        for(int i = 0; i < cellOffsets.length; i += 2){
            short offset = (short) ((cellOffsets[i] << 8) | (cellOffsets[i + 1] & 0xFF));
            this.writeShort(offset - offsetDiff);
        }

        //write the number of cells to destination page
        this.seek((long) destinationPage * pageSize + 0x02);
        this.writeShort(cellsToMove);

        //fill left space with zeroes by the moved cells
        zeroes = new byte[cellOffset - startContent];
        this.seek((long) sourcePage * pageSize + startContent);
        this.write(zeroes);
    }
        
    
    //split the page into two pages where we move half of the records to the new page
    //middle record of original page belongs to parent page and updated necessary pointers
    public int pageSplit(int pageNum, Object splitVal) throws IOException{
        //get the type of the current page index leaf or index interior
        Constants.PageType pageType = getPageType(pageNum);

        //get the parent page number
        int parentPage = getParentPage(pageNum);

        //if the current page is the root, create a new root page
        if(parentPage == 0xFFFFFFFF){
            parentPage = createPage(0xFFFFFFFF, Constants.PageType.INDEX_INTERIOR);

            //write the size of the payload to the leftmost page pointer entry
            this.seek((long) parentPage * pageSize + 0x10);
            this.writeShort(pageSize - 6);

            //write the page pointer and payload size for the rightmost page pointer entry
            this.seek((long) (parentPage + 1) * pageSize - 6);
            //pointer to leftmost page has no corresponding cell payload
            this.writeInt(pageNum);
            this.writeByte(0);

            //update the number of cells in the parent page and set the content start pointer
            this.seek((long) parentPage * pageSize + 0x02);
            this.writeShort(1);
            this.writeShort(pageSize - 6);

            //set parent page number in the original page header
            this.seek((long) pageNum * pageSize + 0x0A);
            this.writeInt(parentPage);
        }

        //create a new page for the right half of the records
        int newPage = createPage(parentPage, pageType);
        //find the middle record of the original page
        int midRecord = getCellCount(pageNum) / 2;
        int midRecordOffset = getCellOffset(pageNum, midRecord);
        
        //read the middle record and write it to parent page
        this.seek((long) pageNum * pageSize + midRecordOffset);
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.readInt();
        }

        //read payload size, record length, value, and associated row IDs
        short payLoadSize = this.readShort();
        byte midRecordLength = this.readByte();
        Object midRecordVal = readData(pageNum, midRecordOffset);
        ArrayList<Integer> midRecordPtrs = new ArrayList<>();
        for(int i = 0; i < midRecordLength; i++){
            midRecordPtrs.add(this.readInt());
        }

        //write the middle record to the parent page
        this.writeCell(midRecordVal, midRecordPtrs, parentPage, newPage);

        //fill space where middle record was with zeroes
        this.seek((long) pageNum * pageSize + midRecordOffset);
        int cellSize;
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            cellSize = payLoadSize + 2 + 4;
        } else {
            cellSize = payLoadSize + 2;
        }
        for(int i = 0; i < cellSize; i++){
            this.writeByte(0);
        }

        //move the cells after the middle record to new page
        this.cellsToPage(pageNum, newPage, midRecord);

        //overwrite the offset of the middle record in old page's offsets array
        this.seek((long) pageNum * pageSize + 0x10 + midRecord * 2);
        this.writeShort(0);

        //get the offset of the remaining cell after the middle record
        int remainCellOffset = getCellOffset(pageNum, midRecord - 1);
        this.seek((long) pageNum * pageSize + 0x02);

        //update the number of cells in original page
        this.writeShort(midRecord);

        //update the content start pointer in the original page
        this.writeShort(remainCellOffset);

        //return the page number of the new page or the original page based on the split value comparison
        if(DataTools.compareCol(dataType, splitVal, midRecordVal) > 0){
            return newPage;
        }else{
            return pageNum;
        }

    }

    //finds the page and index of a specified value in the index file
    //if the cell exists, last element is 1, else the page and index
    //mark the cell that should be before it and last element is 0
    public int[] findPageAndIndex(Object val) throws IOException{
        //start the search from the root page
        int curPage = getRootPage();

        //continue the search until the value is found or the appropriate insert position is determined
        while(true){
            //get the type of the current page index leaf or index interior
            Constants.PageType pageType = getPageType(curPage);

            //find the index of the value in the current page
            int index = findValIndex(val, curPage);

            //if the value is not found, return the current page, insert position, and flag indicating not found
            if(index == -1){
                return new int[] {curPage, 0 ,0};
            }

            //get the offset of the cell in the current page
            int cellOffset = getCellOffset(curPage, index);

            //read the value stored in the cell
            Object cellVal = readData(curPage, cellOffset);

            //compare the value in the cell with the target value
            if(DataTools.compareCol(dataType, val, cellVal) == 0){
                //if the values match, return the current page, index, and flag indicating value found
                return new int[] {curPage, index, 1};
            } else if(pageType == Constants.PageType.INDEX_LEAF){
                //if the current page is a leaf page and values don't match
                //return the current page, index, and flag indicating not found
                return new int[] {curPage, index, 0};
            }else{
                //if the current page is an interior page, update the current page to the child page
                //pointed to by the cell
                this.seek((long) curPage * pageSize + cellOffset);
                curPage = this.readInt();
            }
        }
    }

    //populate the index file by reading records from the associated table and creating index entries
    //retrieve all records from the table, extracts values for the specified column,
    //and creates index entries based on unique values
    public void populateIndex() throws IOException{
        try(TableFile table = new TableFile(tableName, path)){
            //get all records from the table
            ArrayList<Record> records = table.searchUsingCond(-1, null, null);

            //initialize data structures to store unique values and corresponding row IDs
            Set<Object> values = new HashSet<>();
            Map<Object, ArrayList<Integer>> valueToRowID = new HashMap<>();

            //iterate through records and extract values for the specified column
            for(Record rec: records){
                Object val = rec.getValues().get(this.columnIndex);
                //skip null values
                if(val == null){
                    continue;
                }

                //check if the value is encountered for the first time
                if(!values.contains(val)){
                    values.add(val);

                    //initialize the list of row IDs for the value if not present
                    if(!valueToRowID.containsKey(val)){
                        valueToRowID.put(val, new ArrayList<>());
                    }

                    //add the current record's row ID to the list for the value
                    valueToRowID.get(val).add(rec.getRowId());
                }
            }

            //convert unique values to an array and sort them
            Object[] sortedValues = values.toArray();
            Arrays.sort(sortedValues, (a, b) -> DataTools.compareCol(dataType,a,b));

            //iterate through sorted values and write index entries
            for(Object val: sortedValues){
                //find the page and index for the current value in the index
                int[] pageInfo = this.findPageAndIndex(val);
                int page = pageInfo[0];

                //write the index entry for the current value
                this.writeCell(val, valueToRowID.get(val), page, -1);
            }
        }
    }

    //remove a record from a cell using a specified row ID from the index entry associated with given value
    //first identifies the page and index of the index entry containing the value
    //then remove the specified row ID from the index entry
    //val is the the value for which the row ID needs to be removed from the index
    //rowID is the row id to be removed from the index entry
    public void removeFromCell(Object val, int rowID) throws IOException{
        //find the page, index, and existence status of the specified value in the index
        int[] pageInfo = this.findPageAndIndex(val);
        int page = pageInfo[0];
        int index = pageInfo[1];
        int exists = pageInfo[2];

        //check if the record with the specified value exists in the index
        if(exists == 0){
            throw new IllegalArgumentException("Record doesn't exist");
        }

        //remove the specified row ID from the index entry
        removeFromCell(page, index, rowID);
    }

    //Reads and retrieves the list of row IDS associated with the specified page and offset in the index file
    //interprets the index entry structure to extract the relevant row IDs
    public ArrayList<Integer> readRowIDs(int pageNum, int pageOffset) throws IOException{
        //determine the page type whether its index leaf or index interior
        Constants.PageType pageType = getPageType(pageNum);

        //move the file pointer to the specified position within the index file
        this.seek((long) pageNum * pageSize + pageOffset);

        //skip additional bytes if the page type is index interior
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }

        //initialize an arraylist to store the row IDs
        ArrayList<Integer> rowIDs = new ArrayList<>();

        //read the payload size of the index entry
        int payLoadSize = this.readShort();

        //if the payload size is zero, return an empty arraylist with no row IDs
        if(payLoadSize == 0){
            return new ArrayList<>();
        }

        //read the number of row IDs stored in the index entry
        int rowIdNums = this.readByte();

        //read the data type information
        int dataType = this.readByte();

        //skip bytes based on the data type to reach the start of the row IDs
        if(dataType >= 0x0C){
            this.skipBytes(dataType - 0x0C);
        }else{
            this.skipBytes(valueLength);
        }

        //read each row ID and add it to the arraylist
        for(int i = 0; i < rowIdNums; i++){
            rowIDs.add(this.readInt());
        }

        //return the arraylist containing the extracted row IDs
        return rowIDs;
    }

    //delets an entry from a leaf type index page and performs necessary adjustments to maintain page integrity
    public void deleteFromLeafType(int page, int index) throws IOException{
        //check if the content start pointer is greater than half of the available space
        if(getStartContent(page) > (pageSize - 0xF + getCellCount(page) * 2) / 2){
            //retrieve the offset of the specified index within the page
            int pageOffset = getCellOffset(page, index);

            //move the file pointer to the beginning of the specified index entry
            this.seek((long) page * pageSize + pageOffset);

            //read the payload size of the index entry
            int payLoadSize = this.readShort();

            //shifts the cells in the page to remove the specified index entry
            this.cellShift(page, index, -payLoadSize - 4, 0);

            //check if the page is empty after deletion, and if so, delete the page
            if(getCellCount(page) == 0){
                deletePage(page);
            }
        }
    }

    //delete cell from specified index page and cannot have no row id's associated with it
    public void deleteCell(int pageNum, int cellIndex) throws IOException{
        //retrieve the offset of the specified index within the page
        int cellOffset = getCellOffset(pageNum, cellIndex);

        //read the row IDs associated with the cell
        ArrayList<Integer> rowIDs = this.readRowIDs(pageNum, cellOffset);

        //check if the cell has existing row IDs, and if so, throw an exception
        if(rowIDs.size() != 0){
            throw new IllegalArgumentException("Cannot delete cell with existing row ids");
        }

        //check if the page is not the root page, and if so, return because deletion is not allowed here
        if(pageNum > 0){
            return;
        }

        //if the page is leaf and has more than once cell remove the cell from the leaf
        //if the page is an interior page, then check
        //if the page's left child is more than half full, take the last cell from it
        //otherwise if the page's right child is more than half full, take the first cell from it
        //otherwise merge the right child into the left child

        //determine the page type (leaf or interior) to apply the appropriate deletion strategy
        Constants.PageType pageType = getPageType(pageNum);

        if(pageType == Constants.PageType.INDEX_LEAF){
            deleteFromLeafType(pageNum, cellIndex);
        }else{
            deleteFromInteriorType(pageNum, cellIndex);
        }

    }


    //deletes a cell from an interior index page
    //if the deletion causes the left or right child to be less than half,
    //it may trigger a merge or redistribution of cells
    public void deleteFromInteriorType(int pageNum, int cellIndex) throws IOException{
        //retrieve the offset of the specified index within the page
        int pageOffset = getCellOffset(pageNum, cellIndex);

        //read information about the left child
        this.seek((long) pageNum * pageSize + pageOffset);
        int leftChildPage = this.readInt();
        int curPayLoadSize = this.readShort();
        int leftChildSize = getStartContent(leftChildPage);
        int leftChildCellCount = getCellCount(leftChildPage);

        //read information about the right child
        int rightOffset = getCellOffset(pageNum, cellIndex + 1);
        this.seek((long) pageNum * pageSize + rightOffset);
        int rightChildPage = this.readInt();
        int rightChildSize = getStartContent(rightChildPage);
        int rightChildCellCount = getCellCount(rightChildPage);

        //used for replacement cell information
        int payLoadSize;
        int replacePage;
        int replaceIndex;
        byte[] cell;

        //determine which child's cell to replace based on cell count and available space
        if(leftChildSize > (pageSize - 0xF + leftChildCellCount * 2) / 2 && leftChildCellCount > 1){
            replacePage = leftChildPage;
            replaceIndex = leftChildCellCount - 1;
        }else if(rightChildSize > (pageSize - 0xF + rightChildCellCount * 2) / 2 && rightChildCellCount > 1){
            replacePage = rightChildPage;
            replaceIndex = 0;
        }else{
            //merge right child into left child if neither child has enough space
            //take the middle cell and move all cells from right child to left child
            cellsToPage(rightChildPage, leftChildPage, -1);
            deletePage(rightChildPage);

            //update the parent page's information and return
            this.seek((long) pageNum * pageSize + rightOffset);
            this.writeInt(leftChildPage);
            this.cellShift(pageNum, leftChildPage, -4 - curPayLoadSize, 0);
            return;
        }

        //perform the deletion of the cell from the specified child
        deleteCell(replacePage, replaceIndex);

        //retrieve information about the replacement cell
        int replaceOffset = getCellOffset(replacePage, replaceIndex);
        Constants.PageType replacePageType = getPageType(replacePage);
        this.seek((long) leftChildPage * pageSize + replaceOffset);

        //adjust the replacement cell's information in the parent page
        if(replacePageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }

        //read the payload size and cell data
        payLoadSize = this.readShort();
        cell = new byte[payLoadSize];
        this.read(cell);

        //write the replacement cell's information to the parent page
        this.seek((long) pageNum * pageSize + replaceOffset + 4);
        this.writeInt(payLoadSize);

        //if the payload sizes differs, adjust the cell pointers in the parent page
        if(curPayLoadSize != payLoadSize){
            this.cellShift(pageNum, cellIndex, payLoadSize - curPayLoadSize, 0);
        }

        //write the replacement cell data to the parent page
        this.write(cell);
        
    }

    //remove a record from a cell using a specified rowID from a cell within an index page
    public void removeFromCell(int pageNum, int cellIndex, int rowID) throws IOException{
        //retrieve the offset of the specified index within the page and read the existing row IDs in the cell
        int cellOffset = getCellOffset(pageNum, cellIndex);
        ArrayList<Integer> rowIDs = this.readRowIDs(pageNum, cellOffset);

        //check whether row id exists in the cell
        if(!rowIDs.contains(rowID)){
            throw new IllegalArgumentException("Row ID does not exist in cell");
        }
        this.seek((long) pageNum * pageSize + cellOffset);

        //remove the specified row ID from the list of existing row IDs
        rowIDs.remove((Integer) rowID);

        //update the cel with the modified row IDs and adjust the space accordingly
        Constants.PageType pageType = getPageType(pageNum);
        this.seek((long) pageNum * pageSize + cellOffset);

        //if the page is an interior index page, skip the child page pointer
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }

        //read the existing payload size and write the new number of row IDs
        int payLoadSize = this.readShort();
        this.writeByte(rowIDs.size());

        //skip to the end of existing row IDs and rewrite the modified list
        this.skipBytes(payLoadSize - 4 * (rowIDs.size() + 1) - 1);
        for(int j: rowIDs){
            this.writeInt(j);
        }

        //shift the subsequent cells to fill the empty space created by the removal
        this.cellShift(pageNum, cellIndex - 1, -4, 0);

        //delete the cell if it becomes empty after removing the row ID
        if(rowIDs.size() == 0){
            this.deleteCell(pageNum, cellIndex);
        }
    }

    //add record to cell using the specified row ID associated with specific value in index page
    public void addToCell(Object value, int rowID) throws IOException{
        //find the index page and cell index where the record should be added
        int[] pageInfo = findPageAndIndex(value);
        int page = pageInfo[0];
        int index = pageInfo[1];
        int exists = pageInfo[2];

        //if the cell does not exist, create it and add the row ID
        if(exists == 0){
            writeCell(value, new ArrayList<>(Collections.singletonList(rowID)), page, -1);
            return;
        }

        //check if a split is required and perform the split if necessary
        if(split(page, 4)){
            page = pageSplit(page, value);
            index = findValIndex(value, page);
        }

        Constants.PageType pageType = getPageType(page);

        //make space for the new row id within the cell
        this.cellShift(page, index - 1, 4, 0);

        //obtain the offset of the new cell after the shift
        int newCellOffset = getCellOffset(page, index);

        //read the existing row IDs, add the new row ID, and sort the list
        ArrayList<Integer> rowIDs = readRowIDs(page, newCellOffset);
        rowIDs.add(rowID);
        Collections.sort(rowIDs);

        //update the cell content with the modified row IDs
        this.seek((long) page * pageSize + newCellOffset);

        //if the page is an interior index page, skip the child page pointer
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }

        //update the payload size with the addition of the new row ID
        int payLoadSize = this.readShort();
        this.seek((long) page * pageSize + newCellOffset);
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }
        this.writeShort(payLoadSize + 4);

        //update the number of row ids in the cell
        this.writeByte(rowIDs.size());

        //skip bytes based on the data type of the cell value
        int type = this.readByte();
        if(type >= 0x0C){
            this.skipBytes(type - 0x0C);
        }else{
            this.skipBytes(valueLength);
        }

        //write the modified list of row IDs to the cell
        for(int rowid: rowIDs){
            this.writeInt(rowid);
        }
    }


    //traverse the index pages and retrieve the row IDs within a specified range and direction 
    public ArrayList<Integer> retrieveRowIDsInRange(int page, int start, int end, int direction) throws IOException{
        //if the starting index is greater than the ending index, return an empty list
        if(start > end){
            return new ArrayList<>();
        }

        //initialize an empty arraylist to store the retrieved row IDs
        ArrayList<Integer> rowIDs = new ArrayList<>();

        //obtain the page type of the current index page
        Constants.PageType pageType = getPageType(page);

        //initialize the current cell index to the starting index
        int curCell = start;

        //get the offset of the current cell within the page
        int cellOffset = getCellOffset(page, curCell);

        //traverse through the cells within the specified range
        while(curCell <= end){
            //recompute the offset for each cell
            cellOffset = getCellOffset(page, cellOffset);

            //read and add the row IDs from the current cell to the list
            rowIDs.addAll(readRowIDs(page, cellOffset));

            //if the page type is index interior, recursively traverse the child page
            if(pageType == Constants.PageType.INDEX_INTERIOR){
                this.seek((long) page * pageSize + cellOffset);
                int nextPage = this.readInt();
                rowIDs.addAll(retrieveRowIDsInRange(nextPage, 0, getCellCount(nextPage) - 1, direction));
            }
            //move to the next cell
            curCell++;
        }
        //obtain the parent page of the current page
        int parentPage = getParentPage(page);

        //if the parent page is not present when equal to -1, return the accumulated row IDs
        if((parentPage) == -1){
            return rowIDs;
        }
        //find the index of the current cell within the parent page
        int parentIndex = findValIndex(readData(page, cellOffset), parentPage);

        //based on the specified direction, continue the traversal in the parent page
        if(direction == -1){
            //traverse to the left of the parent page up to the identified index
            rowIDs.addAll(retrieveRowIDsInRange(parentPage, 0, parentIndex - 1, direction));
            //add the row IDs from the identified cell in the parent page
            rowIDs.addAll(readRowIDs(parentPage, getCellOffset(parentPage, parentIndex)));
        }else if(direction == 1){
            //traverse to the right of the parent page starting from the identified index + 1
            rowIDs.addAll(retrieveRowIDsInRange(parentPage, parentIndex + 1, getCellCount(parentPage) - 1, direction));
        }else if(direction == 0){
            //traverse both sides of the parent page
            rowIDs.addAll(retrieveRowIDsInRange(parentPage, 0, getCellCount(parentPage) - 1, direction));
        }else{
            //invalid direction
            throw new IllegalArgumentException("Direction must be -1, 0, or 1");
        }

        //return the accumulated row IDs from the traversal
        return rowIDs;
    }

    //searches for row IDs in a specified range based on the given value and operator
    public ArrayList<Integer> searchRowIDsInRange(Object compareVal, String compareOp) throws IOException{
        //finds the page and index information for the given value
        int[] pageInfo = findPageAndIndex(compareVal);
        int page = pageInfo[0];
        int index = pageInfo[1];
        int exists = pageInfo[2];
        int cellOffset = getCellOffset(page, index);

        //perform the search operation based on the specified operator
        switch(compareOp){
            case "=":
            //if the value existsm return the row IDs associated with the cell
                if(exists == 1){
                    return readRowIDs(page, cellOffset);
                }else{
                    //if the value does not exist, return an empty list
                    return new ArrayList<>();
                }
            case "<>":
                //for the not equal operator, retrieve row IDs from both sides of the specified index
                var searchedValues = retrieveRowIDsInRange(page, 0, index - 1, -1);
                searchedValues.addAll(retrieveRowIDsInRange(page, index + 1, getCellCount(page) - 1, 1));
                return searchedValues;
            
            case "<":
                //for the less than or equal operator, retrieve row IDs from the left side of the specified index
                return retrieveRowIDsInRange(page, 0, index - 1, -1);
            
            case "<=":
                //for the less than or equal operator, retrieve row IDs from the left side including the specified index
                return retrieveRowIDsInRange(page, 0, index, -1);
            
            case ">":
                //for the greater than operator, retrieve row IDs from the right side of the specified index
                return retrieveRowIDsInRange(page, index + 1, getCellCount(page) - 1, 1);
            
            case ">=":
                //for the greater than or equal operator, retrieve row IDs from the right side including the specified index
                return retrieveRowIDsInRange(page, index, getCellCount(page) - 1, 1);
            
            default:
                throw new IllegalArgumentException("Operator must be =, <>, <, <=, >, or >=");
        }
    }
}