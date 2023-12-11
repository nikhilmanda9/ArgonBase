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
    short valueSize;
    //name of the table associated with the index
    String tableName;
    //index of the column being indexed
    int columnIndex;
    //file path for the index file
    String path;

    //Creates a new IndexFile object
    public IndexFile(Table table, String columnName, String path) throws IOException {
        super(table.tableName + "." + columnName + ".ndx", Constants.PageType.INDEX_LEAF, path);
        this.tableName = table.tableName;
        this.columnIndex = table.columnNames.indexOf(columnName);
        this.path = path;
        this.dataType = table.getColumnType(columnName);
        //determine the size of the values based on data type
        switch(dataType){
            case TINYINT:
                this.valueSize = 1;
                break;
            
            case SMALLINT:
                this.valueSize = 2;
                break;
            
            case INT:
                this.valueSize = 4;
                break;
            
            case TIME:
                this.valueSize = 4;
                break;
            
            case FLOAT:
                this.valueSize = 4;
                break;
            
            case BIGINT:
                this.valueSize = 8;
                break;
            
            case DATE:
                this.valueSize = 8;
                break;
            
            case DATETIME:
                this.valueSize = 8;
                break;
            
            case DOUBLE:
                this.valueSize = 8;
                break;
            
            case TEXT:
                this.valueSize = -1;
                break;
            
            case NULL:
                this.valueSize = 0;
                break;
        }
    }

    //Reads the data from the specified page and offset in the index file
    public Object readData(int page, int offset) throws IOException{
         //determine the page type of the specified page
        Constants.PageType pageType = getPageType(page);

        //set the file pointer to the specified location in the file based on the page number and offset
        this.seek((long) page * pageSize + offset);

        //if the page is index interior, skip 4 bytes which is possibly a page pointer
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }

        //read the payload size which is the length of the data
        int payloadSize = this.readShort();

        //if the payload size is 0, return null which contains no valid data
        if (payloadSize == 0) {
            return null;
        }

        //skip 1 byte which additional information, possibly a record flag
        this.skipBytes(1);

        //read the record type
        byte recordType = this.readByte();

        //if the record type is 0, return null which is no valid data
        if (recordType == 0) {
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

    //split the page into two pages where we move half of the records to the new page
    //middle record of original page belongs to parent page and updated necessary pointers
    public int pageSplit(int pageNumber, Object splittingValue) throws IOException {
        //get the type of the current page index leaf or index interior
        Constants.PageType pageType = getPageType(pageNumber);

        //get the parent page number
        int parentPage = getParentPage(pageNumber);

        //if the current page is the root, create a new root page
        if (parentPage == 0xFFFFFFFF) {
            parentPage = createPage(0xFFFFFFFF, Constants.PageType.INDEX_INTERIOR);

            //write the size of the payload to the leftmost page pointer entry
            this.seek((long) parentPage * pageSize + 0x10);
            this.writeShort(pageSize - 6);

            //write the page pointer and payload size for the rightmost page pointer entry
            this.seek((long) (parentPage + 1) * pageSize - 6);
            //pointer to leftmost page has no corresponding cell payload
            this.writeInt(pageNumber);
            this.writeByte(0);

            //update the number of cells in the parent page and set the content start pointer
            this.seek((long) parentPage * pageSize + 0x02);
            this.writeShort(1);
            this.writeShort(pageSize - 6);

            //set parent page number in the original page header
            this.seek((long) pageNumber * pageSize + 0x0A);
            this.writeInt(parentPage);
        }

        //create a new page for the right half of the records
        int newPage = createPage(parentPage, pageType);
        //find the middle record of the original page
        int middleRecord = getCellCount(pageNumber) / 2;
        int middleRecordOffset = getCellOffset(pageNumber, middleRecord);

        //read the middle record and write it to parent page
        this.seek((long) pageNumber * pageSize + middleRecordOffset);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.readInt();
        }

         //read payload size, record length, value, and associated row IDs
        short payloadSize = this.readShort();
        byte middleRecordSize = this.readByte();
        Object middleRecordValue = readData(pageNumber, middleRecordOffset);
        ArrayList<Integer> middleRecordPointers = new ArrayList<>();
        for (int i = 0; i < middleRecordSize; i++) {
            middleRecordPointers.add(this.readInt());
        }

        //write the middle record to the parent page
        this.writeCell(middleRecordValue, middleRecordPointers, parentPage, newPage);

        //fill space where middle record was with zeroes
        this.seek((long) pageNumber * pageSize + middleRecordOffset);
        int cellSize = payloadSize + 2 + (pageType == Constants.PageType.INDEX_INTERIOR ? 4: 0);
        for (int i = 0; i < cellSize; i++) {
            this.writeByte(0);
        }

        //move the cells after the middle record to new page
        this.cellsToPage(pageNumber, newPage, middleRecord);

        //overwrite the offset of the middle record in old page's offsets array
        this.seek((long) pageNumber * pageSize + 0x10 + middleRecord * 2);
        this.writeShort(0);

        //get the offset of the remaining cell after the middle record
        int remainingCellOffset = getCellOffset(pageNumber, middleRecord - 1);
        this.seek((long) pageNumber * pageSize + 0x02);


         //update the number of cells in original page
        this.writeShort(middleRecord);
        
        //update the content start pointer in the original page
        this.writeShort(remainingCellOffset);

        //return the page number of the new page or the original page based on the split value comparison
        if (DataTools.compareTo(dataType, splittingValue, middleRecordValue) > 0) {
            return newPage;
        } else {
            return pageNumber;
        }
    }

   //find the index of last cell on the page that has value <= given value
    public int findValueIndex(Object value, int page) throws IOException {
        //get the total number of cells on the specified page
        int numberOfCells = getCellCount(page);

        //if there are no cells on the page, return -1 because value is not found
        if (numberOfCells == 0) {
            return -1;
        }
        
        //perform binary search

        //midpoint
        int mid = numberOfCells / 2;

        //left
        int low = -1;

        //right
        int high = numberOfCells - 1;

        //offset of the current cell
        int currentOffset = getCellOffset(page, mid);

        while (low < high) {
            //read the value at the current offset
            Object currentValue = readData(page, currentOffset);

            //compare the current value with the target value
            int comparison = DataTools.compareTo(dataType, currentValue, value);

            //if the current value is null, consider it smaller than the target value
            if (currentValue == null) {
                comparison = -1;
            }

            //check the result of the comparison
            if (comparison == 0) {
                //target value found, return the current index
                return mid;
            } else if (comparison < 0) {
                //current value is less than target value, adjust the left boundary 
                low = mid;
            } else {
                //current value is greater than the target value, adjust the right boundary 
                high = mid - 1;
            }

            //update the midpoint and current offset for the next iteration
            mid = (int) Math.floor((float) (low + high + 1) / 2f);
            currentOffset = getCellOffset(page, mid);
        }

        //if the loop completes without finding the value, return the last known midpoint
        return mid;
    }

    //move all cells after preceding cell on source page to destination page
    public void cellsToPage(int sourcePage, int destinationPage, int precedingCell) throws IOException {
        int cellOffset = getCellOffset(sourcePage, precedingCell);
        int numberOfCells = getCellCount(sourcePage);
        int numberOfCellsToMove = numberOfCells - precedingCell - 1;
        int contentStart = getStartContent(sourcePage);

        //read the bytes to be moved
        byte[] cellBytes = new byte[cellOffset - contentStart];
        this.seek((long) sourcePage * pageSize + contentStart);
        this.read(cellBytes);

        //read the offsets of the cells to be moved
        byte[] cellOffsets = new byte[(getCellCount(sourcePage) - precedingCell - 1) * 2];
        this.seek((long) sourcePage * pageSize + 0x10 + (precedingCell + 1) * 2L);
        this.read(cellOffsets);

        //overwrite the old cell offsets with zeroes
        this.seek((long) sourcePage * pageSize + 0x10 + (precedingCell + 1) * 2L);
        byte[] zeros = new byte[cellOffsets.length];
        this.write(zeros);

        //write the bytes to be moved
        int newContentStart = getStartContent(destinationPage) - (cellBytes.length);
        this.seek((long) destinationPage * pageSize + newContentStart);
        this.write(cellBytes);

        //write the new content start pointer in the destination page
        this.seek((long) destinationPage * pageSize + 0x04);
        this.writeShort(newContentStart);

        //write the cell offsets that that need to be moved
        int offsetDiff = contentStart - newContentStart;
        this.seek((long) destinationPage * pageSize + 0x10);
        for (int i = 0; i < cellOffsets.length; i += 2) {
            short offset = (short) ((cellOffsets[i] << 8) | (cellOffsets[i + 1] & 0xFF));
            this.writeShort(offset - offsetDiff);
        }

        //write the number of cells to destination page
        this.seek((long) destinationPage * pageSize + 0x02);
        this.writeShort(numberOfCellsToMove);


        //fill left space with zeroes by the moved cells
        zeros = new byte[cellOffset - contentStart];
        this.seek((long) sourcePage * pageSize + contentStart);
        this.write(zeros);
    }


    //populate the index file by reading records from the associated table and creating index entries
    //retrieve all records from the table, extracts values for the specified column,
    //and creates index entries based on unique values
    public void populateIndex() throws IOException {
        try (TableFile table = new TableFile(tableName, path)) {
            //get all records from the table
            ArrayList<Record> records = table.search(-1, null, null);
            
            //initialize data structures to store unique values and corresponding row IDs
            Set<Object> values = new HashSet<>();
            Map<Object, ArrayList<Integer>> valueToRowId = new HashMap<>();

            //iterate through records and extract values for the specified column
            for (Record record : records) {
                Object value = record.getValues().get(this.columnIndex);
                 //skip null values
                if (value == null) {
                    continue;
                }

                //check if the value is encountered for the first time
                if (!values.contains(value)) {
                    values.add(value);

                    //initialize the list of row IDs for the value if not present
                    if (!valueToRowId.containsKey(value)) {
                        valueToRowId.put(value, new ArrayList<>());
                    }

                    //add the current record's row ID to the list for the value
                    valueToRowId.get(value).add(record.getRowId());
                }
            }

            //convert unique values to an array and sort them
            Object[] sortedValues = values.toArray();
            Arrays.sort(sortedValues, (o1, o2) -> DataTools.compareTo(dataType, o1, o2));

            //iterate through sorted values and write index entries
            for (Object value : sortedValues) {
                //find the page and index for the current value in the index
                int[] pageAndIndex = this.findPageAndIndex(value);
                int page = pageAndIndex[0];

                //write the index entry for the current value
                this.writeCell(value, valueToRowId.get(value), page, -1);
            }
        }
    }
    

    //writes a cell to the specified page in the index file, containing the given value, associated row IDS,
    //and optional child page pointer for index interior pages
    public void writeCell(Object value, ArrayList<Integer> rowIds, int page, int childPage) throws IOException {
        
        //determine the page type if it's index leaf or index interior
        Constants.PageType pageType = getPageType(page);

        //calculate payload and cell sizes based on the value type
        short payloadSize = (short) (2 + valueSize + 4 * rowIds.size());


        if (valueSize == -1) {
            payloadSize += ((String) value).length() + 1;
        }

        short cellSize = (short) (2 + payloadSize);

        //adjust cell size for index interior pages
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            cellSize += 4;
        }

        //check if splitting is required and perform split if needed
        if (split(page, cellSize)) {
            page = pageSplit(page, value);
        }

        //find the index where the cell should be inserted
        int insertionPoint = findValueIndex(value, page);
        int offset;
        //if the insert point is the last cell, set the offset to the start of the content
        if (insertionPoint == getCellCount(page) - 1) {
            offset = setStartContent(page, cellSize);
        } else {
            //otherwise, shift cells to accomodate the new cell and get the offset
            offset = cellShift(page, insertionPoint, cellSize, 1);
        }

        //increment the cell count for the page
        incrementCellCount(page);

        //write cell start to cell pointer array
        this.seek((long) page * pageSize + 0x10 + 2L * (insertionPoint + 1));
        this.writeShort(offset);

        //write number of records, data type, and optional child page pointer for index interior pages
        this.seek((long) page * pageSize + offset);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            if (childPage == -1) {
                throw new IOException("Child page not specified");
            }
            this.writeInt(childPage);
        }


        this.writeShort(payloadSize);
        this.writeByte(rowIds.size());


        //write data type
        if (dataType.ordinal() == 0x0C){
            this.writeByte(((String) value).length() + 0x0C);
        } else {
            this.writeByte(dataType.ordinal());
        }
        
        //write the actual value based on the data type
        switch(dataType){
            case TINYINT:
                this.writeByte((Byte) value);
                break;
            case YEAR:
                this.writeByte((Byte) value);
                break;
            case SMALLINT:
                this.writeShort((Short) value);
                break;
            case INT:
                this.writeInt((Integer) value);
                break;
            case TIME:
                this.writeInt((Integer) value);
                break;
            case BIGINT:
                this.writeLong((Long) value);
                break;
            case DATE:
                this.writeLong((Long) value);
                break;
            case DATETIME:
                this.writeLong((Long) value);
                break;
             case FLOAT:
                this.writeFloat((Float) value);
                break;
             case DOUBLE:
                this.writeDouble((Double) value);
                break;
             case TEXT:
                this.writeBytes((String) value);
                break;
        }
        
        //write row IDs associated with the value
        for (int rowId : rowIds) {
            this.writeInt(rowId);
        }
    }


    //remove a record from a cell using a specified row ID from the index entry associated with given value
    //first identifies the page and index of the index entry containing the value
    //then remove the specified row ID from the index entry
    //val is the the value for which the row ID needs to be removed from the index
    //rowID is the row id to be removed from the index entry
    public void removeItemFromCell(Object value, int rowId) throws IOException {
        //find the page, index, and existence status of the specified value in the index
        int[] pageAndIndex = this.findPageAndIndex(value);
        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];

        //check if the record with the specified value exists in the index
        if (exists == 0) {
            throw new IllegalArgumentException("Record does not exist");
        }

        //remove the specified row ID from the index entry
        removeItemFromCell(page, index, rowId);
    }

    //remove a record from a cell using a specified rowID from a cell within an index page
    public void removeItemFromCell(int page, int index, int rowId) throws IOException {
        //retrieve the offset of the specified index within the page and read the existing row IDs in the cell
        int offset = getCellOffset(page, index);
        ArrayList<Integer> rowIds = this.readRowIds(page, offset);
       
        //check whether row id exists in the cell
        if (!rowIds.contains(rowId)) {
            throw new IllegalArgumentException("Row id not present in cell");
        }
        this.seek((long) page * pageSize + offset);
       
        //remove the specified row ID from the list of existing row IDs
        rowIds.remove((Integer) rowId);
        
        //update the cell with the modified row IDs and adjust the space accordingly
        Constants.PageType pageType = getPageType(page);
        this.seek((long) page * pageSize + offset);

        //if the page is an interior index page, skip the child page pointer
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }

        //read the existing payload size and write the new number of row IDs
        int payloadSize = this.readShort();
        this.writeByte(rowIds.size());

        //skip to the end of existing row IDs and rewrite the modified list
        this.skipBytes(payloadSize - 4 * (rowIds.size() + 1) - 1);
        for (int i : rowIds) {
            this.writeInt(i);
        }
        this.cellShift(page, index - 1, -4, 0);

        //shift the subsequent cells to fill the empty space created by the removal
        if (rowIds.size() == 0) {
            this.deleteCell(page, index);
        }
    }

    //delete cell from specified index page and cannot have no row id's associated with it
    public void deleteCell(int page, int index) throws IOException {
        //retrieve the offset of the specified index within the page
        int offset = getCellOffset(page, index);

        //read the row IDs associated with the cell
        ArrayList<Integer> rowIds = this.readRowIds(page, offset);

        //check if the cell has existing row IDs, and if so, throw an exception
        if (rowIds.size() != 0) {
            throw new IllegalArgumentException("Cannot delete cell with row ids");
        }
        
        //check if the page is not the root page, and if so, return because deletion is not allowed here
        if (page > 0) {
            return;
        }

        //if the page is leaf and has more than once cell remove the cell from the leaf
        //if the page is an interior page, then check
        //if the page's left child is more than half full, take the last cell from it
        //otherwise if the page's right child is more than half full, take the first cell from it
        //otherwise merge the right child into the left child

        //determine the page type (leaf or interior) to apply the appropriate deletion strategy
        Constants.PageType pageType = getPageType(page);


        if (pageType == Constants.PageType.INDEX_LEAF) {
            deleteFromLeaf(page, index);
        } else {
            deleteFromInterior(page, index);
        }
    }

    //deletes a cell from an interior index page
    //if the deletion causes the left or right child to be less than half,
    //it may trigger a merge or redistribution of cells
    public void deleteFromInterior(int page, int index) throws IOException {
        //retrieve the offset of the specified index within the page
        int offset = getCellOffset(page, index);

        //read information about the left child
        this.seek((long) page * pageSize + offset);
        int leftChildPage = this.readInt();
        int thisPayloadSize = this.readShort();
        int leftChildSize = getStartContent(leftChildPage);
        int leftChildCells = getCellCount(leftChildPage);

        //read information about the right child
        int rightOffset = getCellOffset(page, index + 1);
        this.seek((long) page * pageSize + rightOffset);
        int rightChildPage = this.readInt();
        int rightChildSize = getStartContent(rightChildPage);
        int rightChildCells = getCellCount(rightChildPage);

        //used for replacement cell information
        int payloadSize;
        int replacementPage;
        int replacementIndex;
        byte[] cell;

        //determine which child's cell to replace based on cell count and available space
        if (leftChildSize > (pageSize - 0xF + leftChildCells * 2) / 2 && leftChildCells > 1) {
            replacementPage = leftChildPage;
            replacementIndex = leftChildCells - 1;
        } else if (rightChildSize > (pageSize - 0xF + rightChildCells * 2) / 2 && rightChildCells > 1) {
            replacementPage = rightChildPage;
            replacementIndex = 0;
        } else {
            //merge right child into left child if neither child has enough space
            //take the middle cell and move all cells from right child to left child
            cellsToPage(rightChildPage, leftChildPage, -1);
            deletePage(rightChildPage);

            //update the parent page's information and return
            this.seek((long) page * pageSize + rightOffset);
            this.writeInt(leftChildPage);
            this.cellShift(page, leftChildPage, -4 - thisPayloadSize, 0);
            return;
        }

        //perform the deletion of the cell from the specified child
        deleteCell(replacementPage, replacementIndex);

        //retrieve information about the replacement cell
        int replacementOffset = getCellOffset(replacementPage, replacementIndex);
        Constants.PageType replacementPageType = getPageType(replacementPage);
        this.seek((long) leftChildPage * pageSize + replacementOffset);

        //adjust the replacement cell's information in the parent page
        if (replacementPageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }

        //read the payload size and cell data
        payloadSize = this.readShort();
        cell = new byte[payloadSize];
        this.read(cell);

        //write the replacement cell's information to the parent page
        this.seek((long) page * pageSize + replacementOffset + 4);
        this.writeInt(payloadSize);
        if (thisPayloadSize != payloadSize) {
            this.cellShift(page, index, payloadSize - thisPayloadSize, 0);
        }

        //write the replacement cell data to the parent page
        this.write(cell);
    }

    //delets an entry from a leaf type index page and performs necessary adjustments to maintain page integrity
    public void deleteFromLeaf(int page, int index) throws IOException {
        //check if the content start pointer is greater than half of the available space
        if (getStartContent(page) > (pageSize - 0xF + getCellCount(page) * 2) / 2) {
            //retrieve the offset of the specified index within the page
            int offset = getCellOffset(page, index);

            //move the file pointer to the beginning of the specified index entry
            this.seek((long) page * pageSize + offset);

            //read the payload size of the index entry
            int payloadSize = this.readShort();

            //shifts the cells in the page to remove the specified index entry
            this.cellShift(page, index, -payloadSize - 4, 0);

            //check if the page is empty after deletion, and if so, delete the page
            if (getCellCount(page) == 0) {
                deletePage(page);
            }
        }

    }

    //add record to cell using the specified row ID associated with specific value in index page
    public void addItemToCell(Object value, int rowId) throws IOException {
        //find the index page and cell index where the record should be added
        int[] pageAndIndex = findPageAndIndex(value);
        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];


        //if the cell does not exist, create it and add the row ID
        if (exists == 0) {
            writeCell(value, new ArrayList<>(Collections.singletonList(rowId)), page, -1);
            return;
        }

        //check if a split is required and perform the split if necessary
        if (split(page, 4)) {
            page = pageSplit(page, value);
            index = findValueIndex(value, page);
        }

        Constants.PageType pageType = getPageType(page);

        //make space for the new row id within the cell
        this.cellShift(page, index - 1, 4, 0);

        //obtain the offset of the new cell after the shift
        int newOffset = getCellOffset(page, index);

        //read the existing row IDs, add the new row ID, and sort the list
        ArrayList<Integer> rowIds = readRowIds(page, newOffset);
        rowIds.add(rowId);
        Collections.sort(rowIds);

        //update the cell content with the modified row IDs
        this.seek((long) page * pageSize + newOffset);

        //if the page is an interior index page, skip the child page pointer
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }

        //update the payload size with the addition of the new row ID
        int payloadSize = this.readShort();
        this.seek((long) page * pageSize + newOffset);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }
        this.writeShort(payloadSize + 4);

        //update the number of row ids in the cell
        this.writeByte(rowIds.size());

        //skip bytes based on the data type of the cell value
        int dataType = this.readByte();
        if (dataType >= 0x0C) {
            this.skipBytes(dataType - 0x0C);
        } else {
            this.skipBytes(valueSize);
        }
        
        //write the modified list of row IDs to the cell
        for (int rowid : rowIds) {
            this.writeInt(rowid);
        }
    }

    //finds the page and index of a specified value in the index file
    //if the cell exists, last element is 1, else the page and index
    //mark the cell that should be before it and last element is 0
    public int[] findPageAndIndex(Object value) throws IOException {
         //start the search from the root page
        int currentPage = getRootPage();
        
        //continue the search until the value is found or the appropriate insert position is determined
        while (true) {
            //get the type of the current page index leaf or index interior
            Constants.PageType pageType = getPageType(currentPage);

            //find the index of the value in the current page
            int index = findValueIndex(value, currentPage);

            //if the value is not found, return the current page, insert position, and flag indicating not found
            if (index == -1) {
                return new int[]{currentPage, 0, 0};
            }

            //get the offset of the cell in the current page
            int offset = getCellOffset(currentPage, index);

            //read the value stored in the cell
            Object cellValue = readData(currentPage, offset);

            //compare the value in the cell with the target value
            if (DataTools.compareTo(dataType, value, cellValue) == 0) {
                //if the values match, return the current page, index, and flag indicating value found
                return new int[] {currentPage, index, 1};
            } else if (pageType == Constants.PageType.INDEX_LEAF) {
                //if the current page is a leaf page and values don't match
                //return the current page, index, and flag indicating not found
                return new int[] {currentPage, index, 0};
            } else {
                //if the current page is an interior page, update the current page to the child page
                //pointed to by the cell
                this.seek((long) currentPage * pageSize + offset);
                currentPage = this.readInt();
            }
        }

    }

    //Reads and retrieves the list of row IDS associated with the specified page and offset in the index file
    //interprets the index entry structure to extract the relevant row IDs
    public ArrayList<Integer> readRowIds(int page, int offset) throws IOException {
        //determine the page type whether its index leaf or index interior
        Constants.PageType pageType = getPageType(page);
        
        //move the file pointer to the specified position within the index file
        this.seek((long) page * pageSize + offset);

        //skip additional bytes if the page type is index interior
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }

        //initialize an arraylist to store the row IDs
        ArrayList<Integer> rowIds = new ArrayList<>();

        //read the payload size of the index entry
        int payloadSize = this.readShort();

        //if the payload size is zero, return an empty arraylist with no row IDs
        if (payloadSize == 0) {
            return new ArrayList<>();
        }

        //read the number of row IDs stored in the index entry
        int numRowIds = this.readByte();

        //read the data type information
        int dataType = this.readByte();

        //skip bytes based on the data type to reach the start of the row IDs
        if (dataType >= 0x0C) {
            this.skipBytes(dataType - 0x0C);
        } else {
            this.skipBytes(valueSize);
        }

        //read each row ID and add it to the arraylist
        for (int i = 0; i < numRowIds; i++) {
            rowIds.add(this.readInt());
        }

        //return the arraylist containing the extracted row IDs
        return rowIds;
    }

    //traverse the index pages and retrieve the row IDs within a specified range and direction 
    public ArrayList<Integer> traverse(int page, int start, int end, int direction) throws IOException {
        //if the starting index is greater than the ending index, return an empty list
        if (start > end) {
            return new ArrayList<>();
        }

        //initialize an empty arraylist to store the retrieved row IDs
        ArrayList<Integer> rowIds = new ArrayList<>();

        //obtain the page type of the current index page
        Constants.PageType pageType = getPageType(page);

        //initialize the current cell index to the starting index
        int currentCell = start;

        //get the offset of the current cell within the page
        int offset = getCellOffset(page, currentCell);

        //traverse through the cells within the specified range
        while (currentCell <= end) {
            //recompute the offset for each cell
            offset = getCellOffset(page, currentCell);

            //read and add the row IDs from the current cell to the list
            rowIds.addAll(readRowIds(page, offset));

            //if the page type is index interior, recursively traverse the child page
            if (pageType == Constants.PageType.INDEX_INTERIOR) {
                this.seek((long) page * pageSize + offset);
                int nextPage = this.readInt();
                rowIds.addAll(traverse(nextPage, 0, getCellCount(nextPage) - 1, direction));
            }

            //move to the next cell
            currentCell++;
        }

        //obtain the parent page of the current page
        int parentPage = getParentPage(page);

        //if the parent page is not present when equal to -1, return the accumulated row IDs
        if (parentPage == -1) {
            return rowIds;
        }

         //find the index of the current cell within the parent page
        int parentIndex = findValueIndex(readData(page, offset), parentPage);
        if (direction == -1) {
            //traverse to the left of the parent page up to the identified index
            rowIds.addAll(traverse(parentPage, 0, parentIndex - 1, direction));
            //add the row IDs from the identified cell in the parent page
            rowIds.addAll(readRowIds(parentPage, getCellOffset(parentPage, parentIndex)));
        } else if (direction == 1) {
            //traverse to the right of the parent page starting from the identified index + 1
            rowIds.addAll(traverse(parentPage, parentIndex + 1, getCellCount(parentPage) - 1, direction));
        } else if (direction == 0) {
            //traverse both sides of the parent page
            rowIds.addAll(traverse(parentPage, 0, getCellCount(parentPage) - 1, direction));
        } else {
            //invalid direction
            throw new IllegalArgumentException("Direction must be -1, 0, or 1");
        }
        //return the accumulated row IDs from the traversal
        return rowIds;
    }

    //searches for row IDs in a specified range based on the given value and operator
    public ArrayList<Integer> search(Object value, String operator) throws IOException {
        //finds the page and index information for the given value
        ArrayList<Integer> rowIds;
        int[] pageAndIndex = findPageAndIndex(value);
        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];
        int offset = getCellOffset(page, index);

        //perform the search operation based on the specified operator
        switch(operator){
            case "=":
            //if the value exists, return the row IDs associated with the cell
                if(exists == 1){
                    rowIds = readRowIds(page, offset);
                }else{
                    //if the value does not exist, return an empty list
                    rowIds = new ArrayList<>();
                }
                break;
            case "<>":
                //for the not equal operator, retrieve row IDs from both sides of the specified index
                var temp = traverse(page, 0, index - 1, -1);
                temp.addAll(traverse(page, index + 1, getCellCount(page) - 1, 1));
                rowIds = temp;
                break;
            
            case "<":
                //for the less than operator, retrieve row IDs from the left side of the specified index
                rowIds = traverse(page, 0, index - 1, -1);
                break;
            case "<=":
                //for the less than or equal operator, retrieve row IDs from the left side including the specified index
                rowIds = traverse(page, 0, index, -1);
                break;
            case ">":
                //for the greater than operator, retrieve row IDs from the right side of the specified index
                rowIds = traverse(page, index + 1, getCellCount(page) - 1, 1);
                break;
            case ">=":
                //for the greater than or equal operator, retrieve row IDs from the right side including the specified index
                rowIds = traverse(page, index, getCellCount(page) - 1, 1);
                break;
            default:
                throw new IllegalArgumentException("Operator must be =, <>, <, <=, >, or >=");
        }

        return rowIds;
    }
}
