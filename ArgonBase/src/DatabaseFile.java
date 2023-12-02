import java.io.*;

public abstract class DatabaseFile extends RandomAccessFile{
    public final int pageSize;
    public int lastPageIndex = -1;

    public DatabaseFile(String name, Constants.PageType pageType, String path) throws IOException{
        super(path + "/" + name, "rw");
        this.pageSize = Constants.PAGE_SIZE;

        //write the first page if the file is empty
        if(this.length() == 0){
            this.createPage(0xFFFFFFFF, pageType);
        }
    }

    public int createPage(int parentPage, Constants.PageType pageType) throws IOException{
        for(int index = 0; index < this.length() / pageSize; index++){
            if(getPageType(index) == Constants.PageType.EMPTY){
                lastPageIndex = index - 1;
            }
        }

        lastPageIndex++;
        this.setLength((long) (lastPageIndex + 1) * pageSize);
        this.seek((long) lastPageIndex * pageSize);

        //page type 0x00
        this.writeByte(pageType.getValue());
        
        //unusued space 0x01
        this.writeByte(0x00);
        
        //number of cells 0x02
        this.writeShort(0x00);

        //start of page content, when there is no content yet, its at the end of the page 0x04
        this.writeShort(pageSize);

        //if its interior page, its rightmost child page
        //if leaf page 0x06, its right sibling page
        this.writeInt(0xFFFFFFFF);

        //parent page 0x0A
        this.writeInt(parentPage);

        //unused space 0x0E
        this.writeInt(0x00);

        //rest of the page would be filled with 0x00
        return lastPageIndex;

    }

    public Constants.PageType getPageType(int page) throws IOException{
        this.seek((long) page * pageSize);
        return Constants.PageType.fromValue(this.readByte());
    }

    public int getParentPage(int page) throws IOException{
        this.seek((long) page * pageSize + 0x0A);
        return this.readInt();
    }

    public int getRootPage() throws IOException{
        int curPage = 0;
        while(getParentPage(curPage) != 0xFFFFFFFF){
            curPage = getParentPage(curPage);
        }

        return curPage;
    }

    public short getStartContent(int page) throws IOException{
        this.seek((long) page * pageSize + 0x04);
        return this.readShort();
    }

    public short setStartContent(int page, short cellSize) throws IOException{
        short oldStartContent = getStartContent(page);
        short newStartContent = (short) (oldStartContent - cellSize);
        this.seek((long) page * pageSize + 0x04);
        this.writeShort(newStartContent);
        return newStartContent;
    }

    public short getCellCount(int page) throws IOException{
        this.seek((long) page * pageSize + 0x02);
        return this.readShort();

    }

    public short incrementCellCount(int page) throws IOException{
        short cellCount = getCellCount(page);
        this.seek((long) page * pageSize + 0x02);
        this.writeShort(cellCount + 1);
        return (short) (cellCount + 1);

    }

    public short getCellOffset(int page, int cellNum) throws IOException{
        if(cellNum == -1){
            return getCellOffset(page, 0);
        }

        this.seek((long) page * pageSize + 0x10 + (2L * cellNum));
        return this.readShort();
    }

    public void deletePage(int page) throws IOException{
        this.seek((long) page * pageSize);
        byte[] emptyPage = new byte[pageSize];
        this.write(emptyPage);
    }

    public int[] getPageInfo(int page) throws IOException{
        int[] pageInfo = new int[5];
        this.seek((long) page * pageSize);

        //page type
        pageInfo[0] = this.readByte();

        this.readByte();

        //number of cells
        pageInfo[1] = this.readShort();

        //content start
        pageInfo[2] = this.readShort();

        //if interior page, its the rightmost child page
        //if leaf page, its the right sibling page
        pageInfo[3] = this.readInt();

        //parent page
        pageInfo[4] = this.readInt();

        return pageInfo;
    }
    public boolean split(int page, int cellSize) throws IOException{
        short cellCount = getCellCount(page);
        short pageHeaderSize = (short) (0x10 + (2 * cellCount + 1));
        return getStartContent(page) - cellSize < pageHeaderSize;
    }

    public int cellShift(int page, int precedingCell, int byteShift, int newRecord) throws IOException{
        if(split(page, byteShift)){
            throw new IOException("Cannot shift cells more than the maximum number of cells page can hold");
        }

        if(precedingCell == getCellCount(page) - 1){
            return setStartContent(page, (short) byteShift);
        }

        int oldStartContent = getStartContent(page);
        int contentOffset = setStartContent(page, (short) byteShift);
        if(contentOffset == pageSize){
            return pageSize - byteShift;
        }

        int startOffset;
        if(precedingCell >= 0){
            startOffset = getCellOffset(page, precedingCell);
        }else{
            startOffset = pageSize;
        }

        this.seek((long) page * pageSize + oldStartContent);
        int byteDiff = startOffset - oldStartContent;

        if(byteShift < 0){
            byteDiff += byteShift;
        }

        byte[] shiftedBytes = new byte[byteDiff];
        this.read(shiftedBytes);

        this.seek((long) page * pageSize + contentOffset);
        this.write(shiftedBytes);

        int cellCount = getCellCount(page);

        //update offsets of shifted cells
        this.seek((long) page * pageSize + 0x10 + (precedingCell + 1) * 2L);
        byte[] oldOffsets = new byte[(cellCount - precedingCell - 1) * 2];
        this.read(oldOffsets);

        //if we are adding a new record, leave room for it's offset
        //if we are removing a record, remove it's offset
        //if we are not chaning the number of records, don't change the offsets
        this.seek((long) page * pageSize + 0x10 + (precedingCell + 1 + newRecord) * 2L);


        for(int i = 0; i < oldOffsets.length; i += 2){
            short oldOffset = (short) ((oldOffsets[i] << 8) | (oldOffsets[i + 1] & 0xFF));
            this.writeShort(oldOffset - byteShift);
        }

        return startOffset - byteShift;

    }
}
