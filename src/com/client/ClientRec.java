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
	
	public static final int TypeByteSize = 1;
	public static final byte Meta = 0;
	public static final byte Sub = 1;
	public static final byte Regular = 2;
	
	// Record = typebyte + length + payload
	
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
		long neededSpace = TypeByteSize + LengthSize + payload.length + SlotSize;
		int maxSize = ChunkServer.ChunkSize - ChunkServer.HeaderSize;
		int num = (int)Math.ceil((double)neededSpace / maxSize);
		
		int maxPayloadSize = maxSize - TypeByteSize - LengthSize - SlotSize;
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
				neededSpace = TypeByteSize + LengthSize + effPayload.length + SlotSize;
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
	
	public RID writeToChunk(byte[] payload, int numRec, int offset, String effHandle, String lastHandle, ByteBuffer header, int type) {
		
		// Write type(2) + length + payload
		byte[] effPayload = new byte[TypeByteSize + LengthSize + payload.length];
		
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
