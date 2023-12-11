import java.io.*;

/*
 * Abstract class representing a database file with common methods for page management
 */
public abstract class DatabaseFile extends RandomAccessFile {
    //page size is in bytes
    public final int pageSize;

    //index of the last used page
    public int lastPageIndex = -1;


    public DatabaseFile(String name, Constants.PageType pageType, String path) throws IOException{
        super(path+"/"+name, "rw");
        this.pageSize = Constants.PAGE_SIZE;
        
        //write the first page if the file is empty
        if (this.length() == 0) {
            this.createPage(0xFFFFFFFF, pageType);
        }
    }


    //Creates a new page with the specified parent page and page type
    //return the index of the created page
    public int createPage(int parentPage, Constants.PageType pageType) throws IOException {
        //iterate through existing pages to find any empty page slot
        for (int p = 0; p < this.length() / pageSize; p++){
            if (getPageType(p) == Constants.PageType.EMPTY) {
                //update lastPageIndex to the index of the last empty page found
                lastPageIndex = p - 1;
            }
        }

        //move to the next available page index
        lastPageIndex++;

        //adjust file length to accomodate the new page
        this.setLength((long) (lastPageIndex + 1) * pageSize);

        //move the file pointer to the beginning of the new page
        this.seek((long) lastPageIndex * pageSize);

         //write page header
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
        this.writeShort(0x00);


        //rest of the page would be filled with 0x00
        return lastPageIndex;
    }

    //returns the starting offset of the content on the specified page
    public short getStartContent(int page) throws IOException {
        //move the file pointer to the location of the content start in the page header
        this.seek((long) page * pageSize + 0x04);
        //read and return the starting offset of the content
        return this.readShort();
    }

    //Retrieves the parent page index of the specified page given the page index
    public int getParentPage(int page) throws IOException {
        //move the file pointer to the location of the parent page information in the page header
        this.seek((long) page * pageSize + 0x0A);

        //read and return the parent page index
        return this.readInt();
    }

    //Retrieves the root page index by iteratively following parent page links until reaching the root
    public int getRootPage() throws IOException {
        //start from the first page
        int currentPage = 0;

        //iterate through parent pages until reaching the root where parent page is 0xFFFFFFFF
        while (getParentPage(currentPage) != 0xFFFFFFFF) {
            currentPage = getParentPage(currentPage);
        }

        //return the index of the root page
        return currentPage;
    }

    //set the starting offset of the content on the specified page after adjusting for the cell size
    //cellLength is the size of the cell to adjust the content start
    public short setStartContent(int page, short cellSize) throws IOException {
        //retrieve the current starting offset of the content
        short oldContentStart = getStartContent(page);
        //calculate the new starting offset after adjusting for the cell length
        short newContentStart = (short) (oldContentStart -  cellSize);
        //move the file pointer to the location of the content start in the page header
        this.seek((long) page * pageSize + 0x04);
        //write the new starting offset to the page header
        this.writeShort(newContentStart);
        //return the new starting offset
        return newContentStart;
    }

    //retrieves the number of cells on the specified page
    public short getCellCount(int page) throws IOException {
        //move the file pointer to the location of the cell count in the page header
        this.seek((long) page * pageSize + 0x02);
        //read and return the number of cells on the page
        return this.readShort();
    }

    //increments the number of cells on the specified page and returns the updated cell count
    public short incrementCellCount(int page) throws IOException {
        //retrieve the current number of cells on the page
        short numberOfCells = getCellCount(page);
        //move the file pointer to the location of the cell count in the page header
        this.seek((long) page * pageSize + 0x02);
        //write the updated cell count to the page header
        this.writeShort(numberOfCells + 1);
        //return the updated number of cells
        return (short) (numberOfCells + 1);
    }

    //Checks whether a page split is needed based on the available free space and the size of a new cell
    //cellLength is the size of the new cell
    public boolean split(int page, int cellSize) throws IOException {
        //retrieve the current number of cells on the page
        short numberOfCells = getCellCount(page);
        //calculate the header size of the page
        short headerSize = (short) (0x10 + (2 * numberOfCells + 1));
        //check if the available free space is less than the required space for the new cell
        return getStartContent(page) - cellSize < headerSize;
    }

    //retrieves the offset of the specified cell on the page
    public short getCellOffset(int page, int cellNumber) throws IOException {
         //if cell number is -1, retrieve the offset of the first cell
        if (cellNumber == -1) {
            return getCellOffset(page, 0);
        }
        //move the file pointer to the location of the cell offset in the page header
        this.seek((long) page * pageSize + 0x10 + (2L * cellNumber));

        //read and return the offset of the specified cell
        return this.readShort();
    }

    //Retrieves information about the specified page
    public int[] getPageInfo(int page) throws IOException {
        //array contains the information: 
        //[page type, number of cells, content start, rightmost child page or right sibling page, parent page]

        //move the file pointer to the beginning of the specified page
        int[] pageInfo = new int[5];
        this.seek((long) page * pageSize);

        //read and store page information in the array

         //page type
        pageInfo[0] = this.readByte(); 

        //unused space
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

        //return the array containing page information
        return pageInfo;
    }

    //Retrieves the page type of the specified page given the page index
    public Constants.PageType getPageType(int page) throws IOException {
        //move the file pointer to the beginning of the specified page
        this.seek((long) page * pageSize);
        //read and return the page type from the page header
        return Constants.PageType.fromValue(this.readByte());
    }

    //Shifts all cells after precedingCell on page to the front of the page by a specified number of bytes
    //page is the page to shift cells on
    //precedingCell is the cell before the first cell to be shifted
    //shift is the number of bytes to shift the cells
    //newRecord is a positive number to add records, negative number to remove records, zero for no change in number of records
    //returns the page offset of the free space created
    public int cellShift(int page, int precedingCell, int shift, int newRecord) throws IOException {
        //check if a split is needed based on the available free space and the size of the new cell
        if (split(page, shift)) {
            throw new IOException("Asked to shift cells more than the page can hold");
        }

        //if the precedingCell is the last cell on the page, adjust the content start
        if (precedingCell == getCellCount(page) - 1) {
            return setStartContent(page, (short) shift);
        }

        //retrieve the current starting offset of the content and set the new content offset
        int oldContentStart = getStartContent(page);
        int contentOffset = setStartContent(page, (short) shift);

        //if the new content offset reaches the end of the page, return the remaining free space
        if (contentOffset == pageSize) {
            return pageSize - shift;
        }

        int startOffset;

        //if precedingCell is greater than or equal to 0, get the offset of the preceding cell
        if (precedingCell >= 0) {
            startOffset = getCellOffset(page, precedingCell);
        } else {
            //if precedingCell is -1, set the startOffset to the end of the page
            startOffset = pageSize;
        }
        //move the file pointer to the location of the data to be shifted
        this.seek((long) page * pageSize + oldContentStart);
        int bytesToMove = startOffset - oldContentStart;

        //if byteshift is negative, adjust the byte difference
        if (shift < 0) {
            bytesToMove += shift;
        }

        //read the bytes to be shifted
        byte[] shiftedBytes = new byte[bytesToMove];
        this.read(shiftedBytes);

        //move the file pointer to the new content offset
        this.seek((long) page * pageSize + contentOffset);
        //write the shifted bytes to the new location
        this.write(shiftedBytes);

        //retrieve the current number of cells on the page
        int numberOfCells = getCellCount(page);
       
         //update offsets of shifted cells
        this.seek((long) page * pageSize + 0x10 + (precedingCell + 1) * 2L);
        byte[] oldOffsets = new byte[(numberOfCells - precedingCell - 1) * 2];
        this.read(oldOffsets);

         //if we are adding a new record, leave room for it's offset
        //if we are removing a record, remove it's offset
        //if we are not chaning the number of records, don't change the offsets
        this.seek((long) page * pageSize + 0x10 + (precedingCell + 1 + newRecord) * 2L);

        for (int i = 0; i < oldOffsets.length; i += 2) {
            short oldOffset = (short) ((oldOffsets[i] << 8) | (oldOffsets[i + 1] & 0xFF));
            //write the adjusted offsets
            this.writeShort(oldOffset - shift);
        }

        //return the page offset of the free space created
        return startOffset - shift;
    }

    //Delete the specified page by filling it with zeroes
    public void deletePage(int page) throws IOException {
        //move the file pointer to the beginning of the specified page
        this.seek((long) page * pageSize);
        //write zeroes to fill the entire page
        byte[] emptyPage = new byte[pageSize];
        this.write(emptyPage);
    }
}