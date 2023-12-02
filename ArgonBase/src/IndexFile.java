import static java.lang.System.lineSeparator;

import java.io.*;
import java.util.*;

public class IndexFile extends DatabaseFile{
    Constants.DataTypes dataType;
    short valueLength;
    String tableName;
    int columnIndex;
    String path;

    public IndexFile(Table table, String columnName, String path) throws IOException{
        super(table.tableName + "." + columnName + ".ndx", Constants.PageType.INDEX_LEAF, path);
        this.tableName = table.tableName;
        this.columnIndex = table.columnNames.indexOf(columnName);
        this.path = path;
        this.dataType = table.getColumnType(columnName);
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

    public Object readData(int page, int pageOffset) throws IOException{
        Constants.PageType pageType = getPageType(page);
        this.seek((long) page * pageSize + pageOffset);
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }

        int payLoadSize = this.readShort();
        if(payLoadSize == 0){
            return null;

        }

        this.skipBytes(1);
        byte recordType = this.readByte();
        if(recordType == 0){
            return null;
        }
        
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
                int textLength = recordType - 0x0C;
                byte[] text = new byte[textLength];
                this.read(text);
                return new String(text);
            
            default:
                return null;
            
        }

    }
    
    //find the index of last cell on the page that has value <= given value
    public int findValIndex(Object val, int page) throws IOException{
        int cellCount = getCellCount(page);
        if(cellCount == 0){
            return -1;
        }

        //midpoint
        int mid = cellCount / 2;
        
        //left
        int low = -1;

        //right
        int high = cellCount - 1;

        int curOffset = getCellOffset(page, mid);

        while(low < high){
            Object curVal = readData(page, curOffset);
            int comparison = DataTools.compareCol(dataType, curVal, val);
            if(curVal == null){
                comparison = -1;
            }

            if(comparison == 0){
                return mid;
            } else if(comparison < 0){
                low = mid;
            } else{
                high = mid - 1;
            }

            mid = (int) Math.floor((float) (low + high + 1) / 2f);
            curOffset = getCellOffset(page, mid);
        }

        return mid;
    }

    //write record to index file
    public void writeCell(Object val, ArrayList<Integer> rowIDs, int page, int childPage) throws IOException{
        Constants.PageType pageType = getPageType(page);

        //get cell size
        short payLoadSize = (short) (2 + valueLength + 4 * rowIDs.size());

        if(valueLength == -1){
            payLoadSize += ((String) val).length() + 1;
        }

        short cellSize = (short) (2 + payLoadSize);
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            cellSize += 4;
        }

        if(split(page, cellSize)){
            page = pageSplit(page, val);
        }

        int insertPoint = findValIndex(val, page);
        int offset;
        if(insertPoint == getCellCount(page) - 1){
            offset = setStartContent(page, cellSize);
        } else{
            offset = cellShift(page, insertPoint, cellSize, 1);
        }

        incrementCellCount(page);

        //write cell start to cell pointer array
        this.seek((long) page * pageSize + 0x10 + 2L * (insertPoint + 1));
        this.writeShort(offset);

        //write number of records
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

        //write value
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

        //write row IDs
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
    //middle record belongs to parent page

    public int pageSplit(int pageNum, Object splitVal) throws IOException{
        Constants.PageType pageType = getPageType(pageNum);

        int parentPage = getParentPage(pageNum);
        if(parentPage == 0xFFFFFFFF){
            parentPage = createPage(0xFFFFFFFF, Constants.PageType.INDEX_INTERIOR);
            this.seek((long) parentPage * pageSize + 0x10);
            this.writeShort(pageSize - 6);
            this.seek((long) (parentPage + 1) * pageSize - 6);
            //pointer to leftmost page has no corresponding cell payload
            this.writeInt(pageNum);
            this.writeByte(0);

            this.seek((long) parentPage * pageSize + 0x02);
            this.writeShort(1);
            this.writeShort(pageSize - 6);

            //set parent to 0xFFFFFFFF
            this.seek((long) pageNum * pageSize + 0x0A);
            this.writeInt(parentPage);
        }

        int newPage = createPage(parentPage, pageType);
        int midRecord = getCellCount(pageNum) / 2;
        int midRecordOffset = getCellOffset(pageNum, midRecord);
        
        //read the middle record and write it to parent page
        this.seek((long) pageNum * pageSize + midRecordOffset);
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.readInt();
        }

        short payLoadSize = this.readShort();
        byte midRecordLength = this.readByte();
        Object midRecordVal = readData(pageNum, midRecordOffset);
        ArrayList<Integer> midRecordPtrs = new ArrayList<>();
        for(int i = 0; i < midRecordLength; i++){
            midRecordPtrs.add(this.readInt());
        }

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

        int remainCellOffset = getCellOffset(pageNum, midRecord - 1);
        this.seek((long) pageNum * pageSize + 0x02);

        //update the number of cells in original page
        this.writeShort(midRecord);

        //update the content start pointer in the original page
        this.writeShort(remainCellOffset);

        if(DataTools.compareCol(dataType, splitVal, midRecordVal) > 0){
            return newPage;
        }else{
            return pageNum;
        }

    }

    //finds the page and index of cell containing value
    //if the cell exists, last element is 1, else the page and index
    //mark the cell that should be before it and last element is 0
    public int[] findPageAndIndex(Object val) throws IOException{
        int curPage = getRootPage();
        while(true){
            Constants.PageType pageType = getPageType(curPage);
            int index = findValIndex(val, curPage);
            if(index == -1){
                return new int[] {curPage, 0 ,0};
            }

            int cellOffset = getCellOffset(curPage, index);
            Object cellVal = readData(curPage, cellOffset);
            if(DataTools.compareCol(dataType, val, cellVal) == 0){
                return new int[] {curPage, index, 1};
            } else if(pageType == Constants.PageType.INDEX_LEAF){
                return new int[] {curPage, index, 0};
            }else{
                this.seek((long) curPage * pageSize + cellOffset);
                curPage = this.readInt();
            }
        }
    }

    //populate the index file with all records in the table
    public void populateIndex() throws IOException{
        try(TableFile table = new TableFile(tableName, path)){
            //get all records
            ArrayList<Record> records = table.searchUsingCond(-1, null, null);
            Set<Object> values = new HashSet<>();
            Map<Object, ArrayList<Integer>> valueToRowID = new HashMap<>();
            for(Record rec: records){
                Object val = rec.getValues().get(this.columnIndex);
                if(val == null){
                    continue;
                }
                if(!values.contains(val)){
                    values.add(val);
                    if(!valueToRowID.containsKey(val)){
                        valueToRowID.put(val, new ArrayList<>());
                    }
                    valueToRowID.get(val).add(rec.getRowId());
                }
            }
            Object[] sortedValues = values.toArray();
            Arrays.sort(sortedValues, (a, b) -> DataTools.compareCol(dataType,a,b));
            for(Object val: sortedValues){
                int[] pageInfo = this.findPageAndIndex(val);
                int page = pageInfo[0];
                this.writeCell(val, valueToRowID.get(val), page, -1);
            }
        }
    }

    //remove a record from a cell
    public void removeFromCell(Object val, int rowID) throws IOException{
        int[] pageInfo = this.findPageAndIndex(val);
        int page = pageInfo[0];
        int index = pageInfo[1];
        int exists = pageInfo[2];
        if(exists == 0){
            throw new IllegalArgumentException("Record doesn't exist");
        }

        removeFromCell(page, index, rowID);
    }

    public ArrayList<Integer> readRowIDs(int page, int pageOffset) throws IOException{
        Constants.PageType pageType = getPageType(page);
        this.seek((long) page * pageSize + pageOffset);
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }
        ArrayList<Integer> rowIDs = new ArrayList<>();
        int payLoadSize = this.readShort();
        if(payLoadSize == 0){
            return new ArrayList<>();
        }

        int rowIdNums = this.readByte();
        int dataType = this.readByte();
        if(dataType >= 0x0C){
            this.skipBytes(dataType - 0x0C);
        }else{
            this.skipBytes(valueLength);
        }

        for(int i = 0; i < rowIdNums; i++){
            rowIDs.add(this.readInt());
        }

        return rowIDs;
    }

    public void deleteFromLeafType(int page, int index) throws IOException{
        if(getStartContent(page) > (pageSize - 0xF + getCellCount(page) * 2) / 2){
            int pageOffset = getCellOffset(page, index);
            this.seek((long) page * pageSize + pageOffset);
            int payLoadSize = this.readShort();
            this.cellShift(page, index, -payLoadSize - 4, 0);
            if(getCellCount(page) == 0){
                deletePage(page);
            }
        }
    }

    //delete cell from index file and cannot have no row id's associated with it
    public void deleteCell(int page, int index) throws IOException{
        int cellOffset = getCellOffset(page, index);
        ArrayList<Integer> rowIDs = this.readRowIDs(page, cellOffset);

        if(rowIDs.size() != 0){
            throw new IllegalArgumentException("Cannot delete cell with existing row ids");
        }

        if(page > 0){
            return;
        }

        //if the page is leaf and has more than once cell remove the cell from the leaf
        //if the page is an interior page, then check
        //if the page's left child is more than half full, take the last cell from it
        //otherwise if the page's right child is more than half full, take the first cell from it
        //otherwise merge the right child into the left child

        Constants.PageType pageType = getPageType(page);
        if(pageType == Constants.PageType.INDEX_LEAF){
            deleteFromLeafType(page, index);
        }else{
            deleteFromInteriorType(page, index);
        }

    }
    public void deleteFromInteriorType(int page, int index) throws IOException{
        int pageOffset = getCellOffset(page, index);
        this.seek((long) page * pageSize + pageOffset);
        int leftChildPage = this.readInt();
        int curPayLoadSize = this.readShort();
        int leftChildSize = getStartContent(leftChildPage);
        int leftChildCellCount = getCellCount(leftChildPage);
        int rightOffset = getCellOffset(page, index + 1);
        this.seek((long) page * pageSize + rightOffset);
        int rightChildPage = this.readInt();
        int rightChildSize = getStartContent(rightChildPage);
        int rightChildCellCount = getCellCount(rightChildPage);

        int payLoadSize;
        int replacePage;
        int replaceIndex;
        byte[] cell;
        if(leftChildSize > (pageSize - 0xF + leftChildCellCount * 2) / 2 && leftChildCellCount > 1){
            replacePage = leftChildPage;
            replaceIndex = leftChildCellCount - 1;
        }else if(rightChildSize > (pageSize - 0xF + rightChildCellCount * 2) / 2 && rightChildCellCount > 1){
            replacePage = rightChildPage;
            replaceIndex = 0;
        }else{
            //merge right child into left child
            //take the middle cell and move all cells from right child to left child
            cellsToPage(rightChildPage, leftChildPage, -1);
            deletePage(rightChildPage);

            this.seek((long) page * pageSize + rightOffset);
            this.writeInt(leftChildPage);
            this.cellShift(page, leftChildPage, -4 - curPayLoadSize, 0);
            return;
        }
        deleteCell(replacePage, replaceIndex);
        int replaceOffset = getCellOffset(replacePage, replaceIndex);
        Constants.PageType replacePageType = getPageType(replacePage);
        this.seek((long) leftChildPage * pageSize + replaceOffset);
        if(replacePageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }

        payLoadSize = this.readShort();
        cell = new byte[payLoadSize];
        this.read(cell);
        this.seek((long) page * pageSize + replaceOffset + 4);
        this.writeInt(payLoadSize);
        if(curPayLoadSize != payLoadSize){
            this.cellShift(page, index, payLoadSize - curPayLoadSize, 0);
        }
        this.write(cell);
        
    }

    //remove a record from a cell
    public void removeFromCell(int page, int index, int rowID) throws IOException{
        int cellOffset = getCellOffset(page, index);
        ArrayList<Integer> rowIDs = this.readRowIDs(page, cellOffset);

        //check whether row id exists in the cell
        if(!rowIDs.contains(rowID)){
            throw new IllegalArgumentException("Row ID does not exist in cell");
        }
        this.seek((long) page * pageSize + cellOffset);

        rowIDs.remove((Integer) rowID);

        //remove the row id from the cell
        // use cellshift to remove space
        // remove the row id from the cell if not empty
        Constants.PageType pageType = getPageType(page);
        this.seek((long) page * pageSize + cellOffset);
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }
        int payLoadSize = this.readShort();
        this.writeByte(rowIDs.size());
        this.skipBytes(payLoadSize - 4 * (rowIDs.size() + 1) - 1);

        for(int j: rowIDs){
            this.writeInt(j);
        }

        this.cellShift(page, index - 1, -4, 0);

        if(rowIDs.size() == 0){
            //delete the cell if empty
            this.deleteCell(page, index);
        }
    }

    //add record to cell
    public void addToCell(Object value, int rowID) throws IOException{
        //find the cell to add the record to
        int[] pageInfo = findPageAndIndex(value);
        int page = pageInfo[0];
        int index = pageInfo[1];
        int exists = pageInfo[2];

        //if the cell does not exist, create it and exit
        if(exists == 0){
            writeCell(value, new ArrayList<>(Collections.singletonList(rowID)), page, -1);
            return;
        }

        if(split(page, 4)){
            page = pageSplit(page, value);
            index = findValIndex(value, page);
        }

        Constants.PageType pageType = getPageType(page);

        //make space for the new row id
        this.cellShift(page, index - 1, 4, 0);

        int newCellOffset = getCellOffset(page, index);

        ArrayList<Integer> rowIDs = readRowIDs(page, newCellOffset);
        rowIDs.add(rowID);
        Collections.sort(rowIDs);

        this.seek((long) page * pageSize + newCellOffset);
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }

        //update the payload size
        int payLoadSize = this.readShort();
        this.seek((long) page * pageSize + newCellOffset);
        if(pageType == Constants.PageType.INDEX_INTERIOR){
            this.skipBytes(4);
        }
        this.writeShort(payLoadSize + 4);

        //update the number of row ids
        this.writeByte(rowIDs.size());

        int type = this.readByte();
        if(type >= 0x0C){
            this.skipBytes(type - 0x0C);
        }else{
            this.skipBytes(valueLength);
        }

        //write the row ids
        for(int rowid: rowIDs){
            this.writeInt(rowid);
        }
    }

    public ArrayList<Integer> traverse(int page, int start, int end, int direction) throws IOException{
        if(start > end){
            return new ArrayList<>();
        }

        ArrayList<Integer> rowIDs = new ArrayList<>();
        Constants.PageType pageType = getPageType(page);
        int curCell = start;
        int cellOffset = getCellOffset(page, curCell);
        while(curCell <= end){
            cellOffset = getCellOffset(page, cellOffset);
            rowIDs.addAll(readRowIDs(page, cellOffset));
            if(pageType == Constants.PageType.INDEX_INTERIOR){
                this.seek((long) page * pageSize + cellOffset);
                int nextPage = this.readInt();
                rowIDs.addAll(traverse(nextPage, 0, getCellCount(nextPage) - 1, direction));
            }
            curCell++;
        }

        int parentPage = getParentPage(page);
        if((parentPage) == -1){
            return rowIDs;
        }

        int parentIndex = findValIndex(readData(page, cellOffset), parentPage);
        if(direction == -1){
            rowIDs.addAll(traverse(parentPage, 0, parentIndex - 1, direction));
            rowIDs.addAll(readRowIDs(parentPage, getCellOffset(parentPage, parentIndex)));
        }else if(direction == 1){
            rowIDs.addAll(traverse(parentPage, parentIndex + 1, getCellCount(parentPage) - 1, direction));
        }else if(direction == 0){
            rowIDs.addAll(traverse(parentPage, 0, getCellCount(parentPage) - 1, direction));
        }else{
            throw new IllegalArgumentException("Direction must be -1, 0, or 1");
        }

        return rowIDs;
    }

    public ArrayList<Integer> search(Object val, String operator) throws IOException{
        ArrayList<Integer> rowIDs;
        int[] pageInfo = findPageAndIndex(val);

        int page = pageInfo[0];
        int index = pageInfo[1];
        int exists = pageInfo[2];
        int cellOffset = getCellOffset(page, index);

        switch(operator){
            case "=":
                if(exists == 1){
                    return readRowIDs(page, cellOffset);
                }else{
                    return new ArrayList<>();
                }
            case "<>":
                var searchedValues = traverse(page, 0, index - 1, -1);
                searchedValues.addAll(traverse(page, index + 1, getCellCount(page) - 1, 1));
                return searchedValues;
            
            case "<":
                return traverse(page, 0, index - 1, -1);
            
            case "<=":
                return traverse(page, 0, index, -1);
            
            case ">":
                return traverse(page, index + 1, getCellCount(page) - 1, 1);
            
            case ">=":
                return traverse(page, index, getCellCount(page) - 1, 1);
            
            default:
                throw new IllegalArgumentException("Operator must be =, <>, <, <=, >, or >=");
        }
    }
}
