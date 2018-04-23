package com.client;

import java.nio.ByteBuffer;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;

public class ClientRec {
	
	// Constants for calculating record size
	public static final int SlotSize = 4;
	public static final int LengthSize = 4;
	
	// 0 = metadata, 1 = sub-record, 2 = regular-record
	public static final int TypeByteSize = 1;
	public static final byte Meta = 0;
	public static final byte Sub = 1;
	public static final byte Regular = 2;
	
	// Record = typebyte + length + payload
	
	// Temporary local chunkserver
	private static ChunkServer cs;
	
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
		int neededSpace = TypeByteSize + LengthSize + payload.length + SlotSize;
		int freeSpace = ChunkServer.ChunkSize - offset - (SlotSize*numRec);
		
		// if the space needed is larger than the size of a chunk
		if (neededSpace > (ChunkServer.ChunkSize - ChunkServer.HeaderSize)) {
			
		}
		else {
			
			// if the space needed fits within the current chunk
			if (neededSpace <= freeSpace) {
				
				// Write type(2) + length + payload
				byte[] effPayload = new byte[TypeByteSize + LengthSize + payload.length];
				effPayload[0] = Regular;
				byte[] payloadSize = ByteBuffer.allocate(4).putInt(payload.length).array();
				
				System.arraycopy(payloadSize, 0, effPayload, 1, payloadSize.length);
				System.arraycopy(payload, 0, header, payloadSize.length, payload.length);
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
				RecordID.setChunkHandle(lastHandle);
				RecordID.setSlotID(slotID);
			}
			else {
				// if the payload does not fit
			}
		}
		
		return null;
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
		return null;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		return null;
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		return null;
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
		return null;
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
		return null;
	}
	
	public int slotIDToSlotOffset(int slotID) {
		return ChunkServer.ChunkSize - (4 * (slotID+1));
	}

}
