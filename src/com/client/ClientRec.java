package com.client;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.master.Master;

import javafx.util.Pair;

public class ClientRec {
	
	// Constants for calculating record size
	public static final int MetaByteSize = 1;
	public static final int SubByteSize = 1;
	
	// Meta vs Regular
	public static final byte Meta = 1;
	public static final byte Regular = 0;
	// Sub vs Entire
	public static final byte Sub = 1;
	public static final byte Entire = 0;
	
	public static final int SlotSize = 4;
	public static final int LengthSize = 4;
	public static final int MaxNonHeaderSize = ChunkServer.ChunkSize - ChunkServer.HeaderSize;
	public static final int MaxRawPayloadSize = MaxNonHeaderSize - MetaByteSize - SubByteSize - LengthSize - SlotSize;
	
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
		
		long neededSpace = MetaByteSize + SubByteSize + LengthSize + payload.length + SlotSize;
		int num = (int)Math.ceil((double)neededSpace / MaxNonHeaderSize);
		
		boolean bigRecord = false;
		// If it is a big record
		if (num > 1)
			bigRecord = true;
		
		Vector<byte[]> subPayloads = new Vector<byte[]>();
		for (int i=0; i<num; i++) {
			// For those full-MaxRawPayloadSize sub-pieces
			int startIndex = i * MaxRawPayloadSize;
			int endIndex = (i+1) * MaxRawPayloadSize;
			
			// If it is the last piece w/ different payload size
			if (i == (num-1)) {
				endIndex = payload.length;
			}			
			
			subPayloads.add(Arrays.copyOfRange(payload, startIndex, endIndex));
		}
		
		Vector<RID> rids = new Vector<RID>();
		
		for (int i = 0; i<subPayloads.size(); i++) {
			byte[] effPayload = subPayloads.get(i);
			
			boolean success = false;
			while(!success) {

				String lastHandle = ofh.getChunkHandles().lastElement();

				// Pick the first location that the chunk is available at
				//String firstLocation = ofh.getChunkLocations().get(lastHandle).firstElement();

				// access the chunk by looking it up with effective handle = location(ip addr)+handle
				String effHandle = lastHandle;

				// Read the header record of the chunk
				ByteBuffer header = ByteBuffer.wrap(cs.readChunk(effHandle, 0, ChunkServer.HeaderSize));

				// Read the number of records
				int numRec = header.getInt();
				// Read the next free offset
				int offset = header.getInt();
				// Read the first slotID
				int firstSlot = header.getInt();
				// Read the last slotID
				int lastSlot = header.getInt();

				// payload + offset info + length info + type info
				neededSpace = MetaByteSize + SubByteSize + LengthSize + effPayload.length + SlotSize;
				int freeSpace = ChunkServer.ChunkSize - offset - (slotIDToSlotOffset(lastSlot));

				// if the space needed fits within the current chunk
				if (neededSpace <= freeSpace) {
					byte subType = Entire;
					if (bigRecord) {
						subType = Sub;
					}
					
					rids.add(writeToChunk(effPayload, numRec, offset, effHandle, lastHandle, Regular, subType, lastSlot, firstSlot));
					
//					// Append to res payload
//					if (bigRecord) {
//						RID curr = rids.lastElement();
//						byte[] temp = res;
//						byte[] handle = curr.getChunkHandle().getBytes();
//						byte[] slot = ByteBuffer.allocate(4).putInt(curr.getSlotID()).array();
//						res = new byte[temp.length+handle.length+slot.length];
//						
//						System.arraycopy(temp, 0, res, 0, temp.length);
//						System.arraycopy(handle, 0, res, temp.length, handle.length);
//						System.arraycopy(slot, 0, res, temp.length+handle.length, slot.length);
//					}
					// Indicate success
					success = true;
				}
				else {
					// if the payload does not fit
					// Pad the chunk
					offset = slotIDToSlotOffset(lastSlot);
					header.putInt(4, offset);
					byte[] headerInfo = header.array();
					cs.writeChunk(effHandle, headerInfo, 4);

					// Tell Master to add another chunk to the file
					master.AddChunk(ofh.getFilePath());
					master.OpenFile(ofh.getFilePath(), ofh);
				}
			}
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
		if (RecordID != null) {
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
		
		String chunkHandle = rec.getRID().getChunkHandle();			
		ByteBuffer header = ByteBuffer.wrap(cs.readChunk(chunkHandle, 0, ChunkServer.HeaderSize));
		if (header == null)
			return ClientFS.FSReturnVals.RecDoesNotExist;
		
		// Read the number of records
		int numRec = header.getInt();
		// Read the next free offset/free slot
		int offset = header.getInt();
		// Read the first record offset
		int firstRec = header.getInt();

		rec.setPayload(cs.readChunk(chunkHandle, rec.getRID().getSlotID(), firstRec));

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
	
	public RID writeToChunk(byte[] payload, int numRec, int offset, String effHandle, String lastHandle, byte metaType, byte subType, int lastSlot, int firstSlot) {
		
		// Write Meta + Sub + length + payload
		byte[] effPayload = new byte[MetaByteSize + SubByteSize + LengthSize + payload.length];
		
		effPayload[0] = metaType;
		effPayload[1] = subType;

		byte[] payloadSize = ByteBuffer.allocate(4).putInt(payload.length).array();

		System.arraycopy(payloadSize, 0, effPayload, 1, payloadSize.length);
		System.arraycopy(payload, 0, effPayload, payloadSize.length, payload.length);
		
		cs.writeChunk(effHandle, effPayload, offset);

		// Write slot ID and the starting offset of its payload
		lastSlot++;
		int slotID = lastSlot;
		byte[] offsetInfo = ByteBuffer.allocate(4).putInt(offset).array();
		cs.writeChunk(effHandle, offsetInfo, slotIDToSlotOffset(slotID));

		// Update header info
		numRec++;
		offset = offset + effPayload.length;
		updateChunkHeader(numRec, offset, firstSlot, lastSlot, effHandle);

		// Update RID		
		RID rid = new RID();
		rid.setChunkHandle(lastHandle);
		rid.setSlotID(slotID);
		
		return rid;
	}
	
	public void updateChunkHeader(int numRec, int offset, int firstSlot, int lastSlot, String chunkHandle) {
		byte[] recordInfo = ByteBuffer.allocate(4).putInt(numRec).array();
		byte[] offsetInfo = ByteBuffer.allocate(4).putInt(offset).array();
		byte[] firstSlotInfo = ByteBuffer.allocate(SlotSize).putInt(firstSlot).array();
		byte[] lastSlotInfo = ByteBuffer.allocate(SlotSize).putInt(lastSlot).array();
		
		byte[] payload = new byte[recordInfo.length + offsetInfo.length + firstSlotInfo.length + lastSlotInfo.length];
		System.arraycopy(recordInfo, 0, payload, 0, recordInfo.length);
		System.arraycopy(offsetInfo, 0, payload, recordInfo.length, offsetInfo.length);
		System.arraycopy(firstSlotInfo, 0, payload, recordInfo.length+offsetInfo.length, firstSlotInfo.length);
		System.arraycopy(lastSlotInfo, 0, payload, recordInfo.length+offsetInfo.length+firstSlotInfo.length, lastSlotInfo.length);
		
		cs.writeChunk(chunkHandle, payload, 0);
		
	}

}
