package com.client;

import java.nio.ByteBuffer;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.master.Master;

import javafx.util.Pair;

public class ClientRec {
	
	// Constants for calculating record size
	public static final int SlotSize = 4;
	public static final int LengthSize = 4;
	
	public static final int MetaByteSize = 1;
	public static final int SubByteSize = 1;
	public static final int Meta = 1;
	public static final int Sub = 1;
	public static final int Regular = 0;
	
	// Record = metabyte + subbyte + length + payload
	
	// Temporary local chunkserver
	private static ChunkServer cs;
	private static Master master;
	
	public ClientRec() {
		
		cs = new ChunkServer();
	}

	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		if (ofh == null)
			return ClientFS.FSReturnVals.BadHandle;
		
		if (RecordID != null)
			return ClientFS.FSReturnVals.BadRecID;
		
		// First specify meta vs record
		
		// Then determine sub or regular
		
		ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
		long neededSpace = MetaByteSize + SubByteSize + LengthSize + payload.length + SlotSize;
		int maxSize = ChunkServer.ChunkSize - ChunkServer.HeaderSize;
		int num = (int)Math.ceil((double)neededSpace / maxSize);
		
		int maxPayloadSize = maxSize - MetaByteSize - SubByteSize - LengthSize - SlotSize;
		int lastChunkSize = (int) (payload.length % maxPayloadSize);
		
		boolean bigRecord = false;
		
		if (num > 1)
			bigRecord = true;
		
		Vector<RID> rids = new Vector<RID>();
		
		int count = 0;
		boolean last = false;
		byte[] res = new byte[0];
		while ((count < num) || last) {
			byte[] effPayload = new byte[maxPayloadSize];
			if (bigRecord) {
				if (count == (num-1)) {
					effPayload = new byte[lastChunkSize];
					payloadBuffer.get(effPayload, num*maxPayloadSize, lastChunkSize);
				}
				else if (last) {
					effPayload = res;
				}
				else
					payloadBuffer.get(effPayload, count*maxPayloadSize, maxPayloadSize);
			}
			else
				effPayload = payload;
			
			boolean success = false;
			while(!success) {

				String lastHandle = ofh.getChunkHandles().lastElement();

				// Pick the first location that the chunk is available at
				//String firstLocation = ofh.getChunkLocations().get(lastHandle).firstElement();

				// access the chunk by looking it up with effective handle = location(ip addr)+handle
				String effHandle = lastHandle;

				// Read the header record of the chunk
				ByteBuffer header = ByteBuffer.wrap(cs.readChunk(effHandle, 0, 8));

				// Read the number of records
				int numRec = header.getInt();
				// Read the next free offset
				int offset = header.getInt();

				// payload + offset info + length info + type info
				neededSpace = MetaByteSize + SubByteSize + LengthSize + effPayload.length + SlotSize;
				int freeSpace = ChunkServer.ChunkSize - offset - (SlotSize*numRec);


				// if the space needed fits within the current chunk
				if (neededSpace <= freeSpace) {
					int type = 2;
					if (bigRecord) {
						type = 1;
						if (last)
							type = 0;
					}
					rids.add(writeToChunk(effPayload, numRec, offset, effHandle, lastHandle, header, type));
					// Append to res payload
					if (bigRecord) {
						RID curr = rids.lastElement();
						byte[] temp = res;
						byte[] handle = curr.getChunkHandle().getBytes();
						byte[] slot = ByteBuffer.allocate(4).putInt(curr.getSlotID()).array();
						res = new byte[temp.length+handle.length+slot.length];
						
						System.arraycopy(temp, 0, res, 0, temp.length);
						System.arraycopy(handle, 0, res, temp.length, handle.length);
						System.arraycopy(slot, 0, res, temp.length+handle.length, slot.length);
					}
					
					// Indicate success
					success = true;
				}
				else {
					// if the payload does not fit
					// Pad the chunk
					offset = slotIDToSlotOffset(numRec - 1);
					header.putInt(4, offset);
					byte[] headerInfo = header.array();
					cs.writeChunk(effHandle, headerInfo, 0);

					// Tell Master to add another chunk to the file
					master.AddChunk(ofh.getFilePath());
					master.OpenFile(ofh.getFilePath(), ofh);
				}
			}
			count++;
			if(last)
				break;
			if ((count == num) && bigRecord)
				last = true;
		}
		
		RecordID = rids.lastElement();
		
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		if (ofh == null) {
			return ClientFS.FSReturnVals.BadHandle;
		}
		if (RecordID == null) {
			return ClientFS.FSReturnVals.BadRecID;
		}
		
		//how do I access payload with what i have to the the size
	//	long freedSpace = TypeByteSize + LengthSize + payload.length + SlotSize;
		int maxSize = ChunkServer.ChunkSize - ChunkServer.HeaderSize;
	//	int num = (int)Math.ceil((double)freedSpace / maxSize);
		
		boolean bigRecord = false;
	//	if (num>1) {bigRecord = true;}
		String targetHandle;
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		for(int i=0;i<ChunkHandles.size();i++) {
			if (ChunkHandles.get(i) == RecordID.getChunkHandle()) {
				targetHandle = ChunkHandles.get(i);
				ByteBuffer header = ByteBuffer.wrap(cs.readChunk(ChunkHandles.get(i),0,ChunkServer.HeaderSize));
				// Read the number of records
				int numRec = header.getInt();
				// Read the next free offset
				int offset = header.getInt();
				// First Rec loc
				int firstRec = header.getInt();
				// Last Rec loc
				int lastRec = header.getInt();
				
				boolean first = false;
				boolean last = false;
				
				//Delete Record
				RecordID = null;
				//Update Header Accordingly
				header.putInt(0,numRec-1);
				//header firstRec
				//header lastRec
				return ClientFS.FSReturnVals.Success;
			}
		}
		return ClientFS.FSReturnVals.RecDoesNotExist;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		if (ofh == null)
			return ClientFS.FSReturnVals.BadHandle;
		
		// wasn't sure how to use ofh, because I thought you could retrieve the chunk handle from the code below
		
		Vector <String>  chunkHandles = ofh.getChunkHandles();	
		String first = chunkHandles.get(0);
		ByteBuffer header = ByteBuffer.wrap(cs.readChunk(first, 0, ChunkServer.HeaderSize));
		if (header == null)
			return ClientFS.FSReturnVals.RecDoesNotExist;
		
		// Read the number of records
		int numRec = header.getInt();
		// Read the next free offset/free slot
		int offset = header.getInt();
		// Read the first record offset
		int firstRec = header.getInt();
		
		ByteBuffer intro = ByteBuffer.wrap(cs.readChunk(first, slotIDToSlotOffset(firstRec), 6));
		byte meta = intro.get();
		byte sub = intro.get();
		int length = intro.getInt();
		if (meta == Meta) {
			
		}
		if(sub ==Sub) {
			
		}
		if(meta == Regular && sub == Regular) {
			cs.readChunk(first, slotIDToSlotOffset(firstRec)+6, length);
		}


		return ClientFS.FSReturnVals.Success;	
		}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		
		if (ofh == null)
			return ClientFS.FSReturnVals.BadHandle;
		
		// wasn't sure how to use ofh, because I thought you could retrieve the chunk handle from the code below
		
		String chunkHandle = rec.getRID().getChunkHandle();			
		ByteBuffer header = ByteBuffer.wrap(cs.readChunk(chunkHandle, 0, 8));
		if (header == null)
			return ClientFS.FSReturnVals.RecDoesNotExist;
		
		// Read the number of records
		int numRec = header.getInt();
		// Read the next free offset/free slot
		int offset = header.getInt();

		rec.setPayload(cs.readChunk(chunkHandle, rec.getRID().getSlotID(), 4));

		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
		
		if (ofh == null)
			return ClientFS.FSReturnVals.BadHandle;

		String chunkHandle = rec.getRID().getChunkHandle();
		ByteBuffer header = ByteBuffer.wrap(cs.readChunk(chunkHandle, 0, 8));
		int slotID = pivot.getSlotID();
		// on 4/23/18 we discussed that our implementation would be to nullify records by setting slotID = -1
		if (header == null || slotID == -1)
			return ClientFS.FSReturnVals.RecDoesNotExist;
		
		// Read the number of records
		int numRec = header.getInt();
		// Read the next free offset/free slot
		int offset = header.getInt();
		
		// pivot trying to access invalid index
		if (slotID > offset)
			return ClientFS.FSReturnVals.RecDoesNotExist;
		
		// pivot trying to read record that may be in next chunk, if RID points to last record of current chunk
		if (slotID == offset) {
			Vector<String> chunkHandles = ofh.getChunkHandles();
			if (chunkHandles.contains(chunkHandle)) {	// idk if we need to check this
				
				int index = -1;	// arbitrary starting value to enter loop
				int i = 0;		// forward iterating index
				// keep searching forwards for an existing record to read from
				while (index != chunkHandles.size()-1) {
					index = chunkHandles.indexOf(chunkHandle)+i;
					if (index == chunkHandles.size()-1)	// invalid pivot, since no record to read after
						return ClientFS.FSReturnVals.RecDoesNotExist;
					
					String nextHandle = chunkHandles.get(index+1);	// index = size-1
					ByteBuffer nextHeader = ByteBuffer.wrap(cs.readChunk(nextHandle, 0, 8));
					
					// unlikely case: chunk handle supposedly created, but no corresponding chunk
					if (nextHeader == null)
						return ClientFS.FSReturnVals.RecDoesNotExist;
					
					// Read the number of records
					int nextNumRec = nextHeader.getInt();
					// Read the next free offset/free slot
					int nextOffset = nextHeader.getInt();
					
					if (nextNumRec != 0) {
						cs.readChunk(nextHandle, offset, 4);	// offset b/c offset-1 for # of records and (offset-1)+1 for next record
						return ClientFS.FSReturnVals.Success;
					}	
				}
				return ClientFS.FSReturnVals.RecDoesNotExist;	// no next records to read exist
				
			}
			else
				return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		
		// case: still in same chunk
		rec.setPayload(cs.readChunk(chunkHandle, slotID+1, 4));
				
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
	 * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
		
		if (ofh == null)
			return ClientFS.FSReturnVals.BadHandle;

		String chunkHandle = rec.getRID().getChunkHandle();
		ByteBuffer header = ByteBuffer.wrap(cs.readChunk(chunkHandle, 0, 8));
		int slotID = pivot.getSlotID();
		// on 4/23/18 we discussed that our implementation would be to nullify records by setting slotID = -1
		if (header == null || slotID == -1)
			return ClientFS.FSReturnVals.RecDoesNotExist;
		
		// Read the number of records
		int numRec = header.getInt();
		// Read the next free offset/free slot
		int offset = header.getInt();
		
		// pivot trying to access invalid index
//		if (slotID < header size)
//			return ClientFS.FSReturnVals.RecDoesNotExist;
		
		// pivot trying to read record that may be in prev chunk, if RID points to last record of current chunk
		if (slotID == offset) {
			Vector<String> chunkHandles = ofh.getChunkHandles();
			if (chunkHandles.contains(chunkHandle)) {	// idk if we need to check this
				
				int index = -1; //arbitrary starting value to enter loop
				int i = 0;		//backwards iterating index
				// keep searching backwards for an existing record to read from
				while (index != 0) {
					index = chunkHandles.indexOf(chunkHandle)-i;
					if (index == 0)	// invalid pivot, since no record to read before
						return ClientFS.FSReturnVals.RecDoesNotExist;
					i++;
					
					String prevHandle = chunkHandles.get(index-i-1);
					ByteBuffer prevHeader = ByteBuffer.wrap(cs.readChunk(prevHandle, 0, 8));
					int prevNumRecords = ByteBuffer.wrap(cs.readChunk(prevHandle, 0, 4)).getInt();
					
					// unlikely case: chunk handle supposedly created, but no corresponding chunk
					if (prevHeader == null)
						return ClientFS.FSReturnVals.RecDoesNotExist;
					
					// Read the number of records
					int prevNumRec = prevHeader.getInt();
					// Read the next free offset/free slot
					int prevOffset = prevHeader.getInt();
					
					if (prevNumRec != 0) {
						TinyRec prevRec = rec;
						prevRec.getRID().setChunkHandle(prevHandle);
						ClientFS.FSReturnVals returnVal = ReadLastRecord(ofh, prevRec);
						if (returnVal != ClientFS.FSReturnVals.Success)	// file is empty/bad handle
							return returnVal;
						return ClientFS.FSReturnVals.Success;
					}
				}
				return ClientFS.FSReturnVals.RecDoesNotExist;	// no prev records to read exist
			}
			else
				return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		
		// case: still in same chunk
		rec.setPayload(cs.readChunk(chunkHandle, slotID-2, 4));
				
		return ClientFS.FSReturnVals.Success;
	}
	
	public int slotIDToSlotOffset(int slotID) {
		return ChunkServer.ChunkSize - (4 * (slotID+1));
	}
	
	public RID writeToChunk(byte[] payload, int numRec, int offset, String effHandle, String lastHandle, ByteBuffer header, int type) {
		
		// Write type(2) + length + payload
		byte[] effPayload = new byte[MetaByteSize + SubByteSize + LengthSize + payload.length];
		
		switch (type) {
		case 0:
			effPayload[0] = Meta;
			break;
		case 1:
			effPayload[0] = Sub;
			break;
		case 2:
			effPayload[0] = Regular;
			break;
		default:
			break;
		}

		byte[] payloadSize = ByteBuffer.allocate(4).putInt(payload.length).array();

		System.arraycopy(payloadSize, 0, effPayload, 1, payloadSize.length);
		System.arraycopy(payload, 0, effPayload, payloadSize.length, payload.length);
		cs.writeChunk(effHandle, effPayload, offset);

		// Write slot ID and the starting offset of its payload
		int slotID = numRec;
		byte[] offsetInfo = ByteBuffer.allocate(4).putInt(offset).array();
		cs.writeChunk(effHandle, offsetInfo, slotIDToSlotOffset(slotID));

		// Update header info
		numRec++;
		offset = offset + effPayload.length;
		header.putInt(0, numRec);
		header.putInt(4, offset);
		byte[] headerInfo = header.array();
		cs.writeChunk(effHandle, headerInfo, 0);

		// Update RID		
		RID rid = new RID();
		rid.setChunkHandle(lastHandle);
		rid.setSlotID(slotID);
		
		return rid;
	}

}
