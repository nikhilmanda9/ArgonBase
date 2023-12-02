import java.io.*;
import java.util.*;

public class TableFile extends DatabaseFile{
    
    public TableFile(String tableName, String path) throws IOException{
        super(tableName + ".tbl", Constants.PageType.TABLE_LEAF, path);
    }

    //find the smallest row id
    public int getSmallestRowID(int page) throws IOException{
        if(getCellCount(page) <= 0){
            throw new IOException("Empty page");
        }

        Constants.PageType pageType = getPageType(page);
        int offset = getCellOffset(page, 0);

        this.seek((long) page * pageSize + offset);

        if(pageType == Constants.PageType.TABLE_LEAF){
            this.skipBytes(2);
        } else if(pageType == Constants.PageType.TABLE_INTERIOR){
            this.skipBytes(4);
        }

        return this.readInt();
    }

    //find the last row id
    public int getLastRowID() throws IOException{
        int lastPage = getLastLeafPage();
        int offset = getStartContent(lastPage);

        int cellCount = getCellCount(lastPage);
        if(cellCount == 0){
            return -1;
        }

        Constants.PageType pageType = getPageType(lastPage);
        this.seek((long) lastPage * pageSize + offset);

        if(pageType == Constants.PageType.TABLE_LEAF){
            this.skipBytes(2);
        }else if(pageType == Constants.PageType.TABLE_INTERIOR){
            this.skipBytes(4);
        }

        return this.readInt();
    }

    public int getLastLeafPage() throws IOException{
        int nextPage = getRootPage();
        while(true){
            Constants.PageType pageType = getPageType(nextPage);
            if(pageType == Constants.PageType.TABLE_LEAF){
                break;
            }
            this.seek((long) pageSize * nextPage + 0x06);
            nextPage = this.readInt();
        }

        return nextPage;
    }

    //split page into two pages to mimic b+1 tree
    //no records are moved and if the root page is split, new root page is created
    public int pageSplit(int pageNum, int splitRowID) throws IOException{
        Constants.PageType pageType = getPageType(pageNum);

        int parentPage = getParentPage(pageNum);

        if(parentPage == 0xFFFFFFFF){
            parentPage = createPage(0xFFFFFFFF, Constants.PageType.TABLE_INTERIOR);
            this.seek((long) pageNum * pageSize + 0x0A);
            this.writeInt(parentPage);
            writePagePtr(parentPage, pageNum, getSmallestRowID(pageNum));    
        }

        int newPage = createPage(parentPage, pageType);
        writePagePtr(parentPage, newPage, splitRowID);
        if(pageType == Constants.PageType.TABLE_LEAF){
            this.seek((long) pageNum * pageSize + 0x06);
            this.writeInt(newPage);
        }

        return newPage;

    }

    //write page pointer to interior page in format [page num][row id]
    public void writePagePtr(int page, int ptr, int rowID) throws IOException{
        short cellSize = 8;
        if(split(page, cellSize)){
            page = pageSplit(page, rowID);
        }

        short startContent = setStartContent(page, cellSize);
        int cellCount = incrementCellCount(page);

        this.seek((long) page * pageSize + 0x06);

        //write pointer to rightmost child
        this.writeInt(ptr);

        this.seek((long) page * pageSize + 0x0E + cellCount * 2);

        //write to cell pointer array
        this.writeShort(startContent);

        this.seek((long) page * pageSize + startContent);

        //write cell
        this.writeInt(ptr);

        this.writeInt(rowID);
    }

    public void writeData(Constants.DataTypes datatype, Object obj) throws IOException{
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

    private int getRowID(int page, int pageIndex) throws IOException{
        Constants.PageType pageType = getPageType(page);

        //page offset is the location of the row as number of bytes from beginning of page
        int pageOffset = getCellOffset(page, pageIndex);

        this.seek((long) page * pageSize + pageOffset);

        if(pageType == Constants.PageType.TABLE_INTERIOR){
            //skip page pointer
            this.skipBytes(4);
        }else{
            //skip record size
            this.skipBytes(2);
        }

        return this.readInt();
    }


    public int findPageRecord(int page, int rowID) throws IOException{
        int cellCount = getCellCount(page);

        //midpoint
        int curCell = cellCount / 2;

        //left
        int low = 0;

        //right
        int high = cellCount - 1;

        int curRowID = getRowID(page, curCell);

        //binary search over cells
        while(low < high){
            //0x10 is location of cells in the page where each cell location is 2 bytes
            if(curRowID < rowID){
                //might be inside the current cell
                low = curCell;
            } else if(curRowID > rowID){
                //-1 must be left out of the current cell
                high = curCell - 1;
            } else{
                break;
            }

            curCell = (low + high + 1) / 2;
            curRowID = getRowID(page, curCell);
        }

        return curCell;
    }

    public void writeRecord(Record rec, int page) throws IOException{
        short recordLength = rec.getRecordLength();
        short cellSize = (short) (recordLength + 6);
        if(split(page, cellSize)){
            page = pageSplit(page, rec.getRowId());
        }

        short startContent = this.setStartContent(page, cellSize);
        short cellCount = incrementCellCount(page);
        this.seek((long) pageSize * page + 0x0E + 2 * cellCount);

        //write to cell pointer array
        this.writeShort(startContent);
        this.seek((long) pageSize * page + startContent);
        ArrayList<Constants.DataTypes> columns = rec.getColumns();

        ArrayList<Object> values = rec.getValues();

        //write to cell
        this.writeShort(recordLength);
        this.writeInt(rec.getRowId());

        byte[] pageHeader = rec.getPageHeader();
        //write record
        this.write(pageHeader);

        for(int i = 0; i < columns.size(); i++){
            writeData(columns.get(i), values.get(i));
        }

    }
    public void updatePagePtr(int page, int pageIndex, int newRowID) throws IOException{
        int cellOffset = getCellOffset(page, pageIndex);
        this.seek((long) page * pageSize + cellOffset + 4);
        int oldRowID = this.readInt();
        this.seek((long) page * pageSize + cellOffset + 4);
        this.writeInt(newRowID);
        if (pageIndex == 0){
            int parentPage = getParentPage(page);
            int parentIndex = findPageRecord(parentPage, oldRowID);
            updatePagePtr(parentPage, parentIndex, newRowID);
        }

    }

    public int[] findRecord(int rowID) throws IOException{
        int curPage = getRootPage();
        while(true){
            Constants.PageType pageType = getPageType(curPage);
            int curCell = findPageRecord(curPage, rowID);
            int curRowID = getRowID(curPage, curCell);
            if(pageType == Constants.PageType.TABLE_LEAF){
                if(curRowID == rowID){
                    return new int[] {curPage, curCell, 1};
                }else{
                    return new int[] {curPage, curCell, 0};
                }
            }else if(pageType == Constants.PageType.TABLE_INTERIOR){
                int pageOffset = getCellOffset(curPage, curCell);
                this.seek((long) curPage * pageSize + pageOffset);
                curPage = this.readInt();
            }
        }
    }

    //read a record
    private Record readRecord(int page, int pageOffset) throws IOException{
        this.seek((long) page * pageSize + pageOffset);
        //record size
        this.readShort();
        int rowID = this.readInt();
        byte numColumns = this.readByte();
        byte[] cols = new byte[numColumns];
        for(int i = 0; i < numColumns; i++){
            cols[i] = this.readByte();
        }

        ArrayList<Object> values = new ArrayList<>();
        ArrayList<Constants.DataTypes> columnTypes = new ArrayList<>();

        for(byte b: cols){
            Constants.DataTypes dataType;
            if(b > 0x0C){
                dataType = Constants.DataTypes.TEXT;
            }else{
                dataType = Constants.DataTypes.values()[b];
            }

            columnTypes.add(dataType);

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
                    int textLength = b - 0x0C;
                    byte[] text = new byte[textLength];
                    this.readFully(text);
                    values.add(new String(text));
                    break;
                case NULL:
                    values.add(null);
                    break;
            }
        }

        return new Record(columnTypes, values, rowID);
    }

    //search all records that match a given condition
    public ArrayList<Record> searchUsingCond(int columnIndex, Object val, String operator) throws IOException{
        ArrayList<Record> records = new ArrayList<>();

        int curPage = getRootPage();
        Constants.PageType pageType = getPageType(curPage);

        //go to first leaf page
        while(pageType != Constants.PageType.TABLE_LEAF){
            int pageOffset = getCellOffset(curPage, 0);
            this.seek((long) curPage * pageSize + pageOffset);
            curPage = this.readInt();
            pageType = getPageType(curPage);
        }

        //iterate over all records in leaf pages
        while(curPage != 0xFFFFFFFF){
            this.seek((long) curPage * pageSize);
            int cellCount = getCellCount(curPage);

            //iterate over all records in current page
            for(int i = 0; i < cellCount; i++){
                this.seek((long) curPage * pageSize + 0x10 + 2 * i);
                int curOffset = getCellOffset(curPage, i);
                Record record = readRecord(curPage, curOffset);
                if(record.compare(columnIndex, val, operator)){
                    records.add(record);
                }
            }

            this.seek((long) curPage * pageSize + 0x06);
            curPage = this.readInt();
        }

        return records;
    }


    public void updateRecord(int rowID, int columnIndex, Object newVal) throws IOException{
        int[] pageInfo = findRecord(rowID);
        int page = pageInfo[0];
        int pageIndex = pageInfo[1];
        int exists = pageInfo[2];
        if(exists == 0){
            throw new IOException("Record doesn't exist");
        }

        int pageOffset = getCellOffset(page, pageIndex);
        Record rec = readRecord(page, pageOffset);
        if(rec.getColumns().get(columnIndex) == Constants.DataTypes.TEXT){
            int oldSize = ((String) rec.getValues().get(columnIndex)).length();
            int newSize = ((String) newVal).length();
            if(split(page, (short) (newSize - oldSize))){
                pageSplit(page, rowID);
                int[] newPageInfo = findRecord(rowID);
                page = newPageInfo[0];
                pageIndex = newPageInfo[1];
            }

            this.cellShift(page, pageIndex - 1, oldSize - newSize, 0);
            pageOffset = getCellOffset(page, pageIndex);
        }

        ArrayList<Object> values = rec.getValues();
        values.set(columnIndex, newVal);
        Record newRec = new Record(rec.getColumns(), values, rec.getRowId());
        this.seek((long) page * pageSize + pageOffset + 6);
        byte[] pageHeader = newRec.getPageHeader();
        this.write(pageHeader);
        for(int i = 0; i < newRec.getColumns().size(); i++){
            writeData(newRec.getColumns().get(i), values.get(i));
        }
    }

    public Record getRecord(int rowID) throws IOException{
        int[] pageInfo = findRecord(rowID);
        int page = pageInfo[0];
        int index = pageInfo[1];
        int exists = pageInfo[2];
        if(exists == 0){
            return null;
        }

        int cellOffset = getCellOffset(page, index);
        return readRecord(page, cellOffset);
    }
    public void deleteRecord(int rowID) throws IOException{
        int[] pageInfo = findRecord(rowID);
        int page = pageInfo[0];
        int pageIndex = pageInfo[1];
        int exists = pageInfo[2];
        if(exists == 0){
            throw new IOException("Record doesn't exist");
        }

        int pageOffset = getCellOffset(page, pageIndex);
        this.seek((long) page * pageSize + pageOffset);
        short payloadSize = this.readShort();
        this.cellShift(page, pageIndex - 1, -payloadSize - 6, -1);
        if(pageIndex == 0 && getParentPage(page) != 0xFFFFFFFF){
            updatePagePtr(getParentPage(page), pageIndex, getSmallestRowID(page));
        }

        //decrement cell count
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
