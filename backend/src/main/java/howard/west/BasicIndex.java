// package cs276.assignments;
package howard.west;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.lang.Exception;
import java.util.List;

public class BasicIndex implements BaseIndex {
	private static final int INT_SIZE = 4;

	@Override
	public PostingList readPosting(FileChannel fc) throws IOException {
		/*
		 * Allocate two ints, preparing for reading in termId and freq
		 */
		ByteBuffer buffer = ByteBuffer.allocate(INT_SIZE * 2);
		int numOfBytesRead;

		/*
		 * fc.read reads a sequence of bytes from the fc channel into 
		 * buffer. Bytes are read starting at this channel's current 
		 * file position, and then the file position is updated 
		 * with the number of bytes actually read. 
		 */
		try {
			numOfBytesRead = fc.read(buffer);
			if (numOfBytesRead == -1) return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw e;
		}
		/*
		 * Rewinds the buffer. Position is set to zero. 
		 * We are ready to get our termId and frequency.
		 */
		buffer.rewind();
		/*
		 * Reads the next four bytes at buffer's current position, 
		 * composing them into an int value according to the 
		 * current byte order, and then increments the position 
		 * by four.
		 */	
		int termId = buffer.getInt();
		int freq = buffer.getInt();

		ByteBuffer docIdBuffer = ByteBuffer.allocate(INT_SIZE * freq);
		int docIdNumOfBytesRead = fc.read(docIdBuffer);
		if (docIdNumOfBytesRead == -1) return null;
		docIdBuffer.rewind();

		PostingList posting = new PostingList(termId);
		List<Integer> list = posting.getList();
		for (int i = 0; i < freq; ++i) {
			list.add(docIdBuffer.getInt());
		}

		return posting;

	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) throws IOException {
		/*
		 * The allocated space is for termID + freq + docIds in p
		 */
		ByteBuffer buffer = ByteBuffer.allocate(INT_SIZE * (p.getList().size() + 2));
		buffer.putInt(p.getTermId()); // put termId
		buffer.putInt(p.getList().size()); // put freq
		for (int id : p.getList()) { // put docIds
			buffer.putInt(id); 
		}
		
		/* Flip the buffer so that the position is set to zero.
		 * This is the counterpart of buffer.rewind()
		 */
		buffer.flip();
		/*
		 * fc.write writes a sequence of bytes into fc from buffer.
		 * File position is updated with the number of bytes actually 
		 * written
		 */
		try {
			fc.write(buffer);
		} catch (IOException e) {
			throw e;
		}
	}
}
