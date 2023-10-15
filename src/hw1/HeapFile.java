package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	private File file;
	private int id;
	private TupleDesc td;
	
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		this.file = f;
		this.td = type;
		this.id = hashCode();
	}
	
	public File getFile() {
		//your code here
		return this.file;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.td;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		byte [] data = new byte[PAGE_SIZE];
		HeapPage newHP = null;
		try {
			// read the file, for the length of data
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			raf.seek(PAGE_SIZE * id);
			raf.read(data); 
			raf.close();
			newHP = new HeapPage(id, data, this.getId());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return newHP;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return this.id;
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		//your code here
		byte [] data = p.getPageData();
		
		try {
			// read the file, for the length of data
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			raf.seek(PAGE_SIZE * p.getId());
			raf.write(data); 
			raf.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		//your code here
		// Find a page with an open slot
			// read each page of the heapfile
		for (int i = 0; i < getNumPages(); i++) {
			HeapPage hp = readPage(i);
			// for each heappage, look for available slots
			for (int j =0; j < hp.getNumSlots(); j++) {
				if (!hp.slotOccupied(j)) {
					try {
						hp.addTuple(t);
						this.writePage(hp);
						return hp;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} 
			}
		}
		
		// or create a new page if all pages are full
		HeapPage newHP = null;
		try {
			newHP = new HeapPage(getNumPages(), new byte[PAGE_SIZE], this.getId());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// passes the tuple to this page to be stored
		try {
			newHP.addTuple(t);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// writes the page to disk.
		this.writePage(newHP);
		return newHP;
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		HeapPage hp = this.readPage(t.getPid());
		try {
			hp.deleteTuple(t);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.writePage(hp);
		
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> arrTuple = new ArrayList<>();
		for(int i = 0; i < getNumPages(); i++) {
			HeapPage hp = readPage(i);
			Iterator<Tuple> tuples = hp.iterator();
			
			while(tuples.hasNext()) {
				arrTuple.add(tuples.next());
			}
		}
		return arrTuple;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		return (int) Math.ceil(file.length()/PAGE_SIZE);
	}
}