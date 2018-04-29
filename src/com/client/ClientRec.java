package com.client;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.master.Master;

import javafx.util.Pair;

public class ClientRec {
	
	// Header info offset
	public static final int HeaderOffset = 0;
	public static final int HeaderFirstSlotIDOffset = 8;
	public static final int HeaderLastSlotIDOffset = 12;
	
	// Constants for calculating record size
	public static final int MetaByteSize = 1;
	public static final int SubByteSize = 1;
	public static final int LengthSize = 4;
	public static final int IntroSize = MetaByteSize + SubByteSize + LengthSize;
	
	// Meta vs Regular
	public static final byte Meta = 1;
	public static final byte Regular = 0;
	// Sub vs Entire
	public static final byte Sub = 1;
	public static final byte Entire = 0;
	
	public static final int SlotSize = 4;
	public static final int MaxNonHeaderSize = ChunkServer.ChunkSize - ChunkServer.HeaderSize;
	public static final int MaxRawPayloadSize = MaxNonHeaderSize - MetaByteSize - SubByteSize - LengthSize - SlotSize;
	
	public static final int SlotNullified = -1;
	
	// Record = metabyte + subbyte + length + payload
	
	// Temporary local chunkserver
	private static ChunkServer cs;
	private static Master master;
	
	public ClientRec() {
		
		cs = new ChunkServer();
	}
	
	// Temporary Constructor
	public ClientRec(Master master, ChunkServer cs) {
		this.master = master;
		this.cs = cs;
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
		
		if (RecordID != null) {
			if (RecordID.getChunkHandle() != null)
				return ClientFS.FSReturnVals.BadRecID;
		}
		
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
				int freeSpace = (slotIDToSlotOffset(lastSlot)) - offset;

				// if the space needed fits within the current chunk
				if (neededSpace <= freeSpace) {
					byte subType = Entire;
					if (bigRecord) {
						subType = Sub;
					}
					
					rids.add(writeToChunk(effPayload, numRec, offset, effHandle, lastHandle, Regular, subType, lastSlot, firstSlot));
					
					// Indicate success
					success = true;
				}
				else {
					// if the payload does not fit
					// Pad the chunk
					offset = slotIDToSlotOffset(lastSlot);
					byte[] headerInfo = ByteBuffer.allocate(4).putInt(offset).array();
					cs.writeChunk(effHandle, headerInfo, 4);

					// Tell Master to add another chunk to the file
					master.AddChunk(ofh.getFilePath());
					master.OpenFile(ofh.getFilePath(), ofh);
				}
			}
		}
		
		// Keep appending the meta records as long as the "rids" vector is long
		while (rids.size() > 1) {
			
			// Initialize the "metaPayload" array and its temp copy
			byte[] metaPayload = new byte[0];
			byte[] tempPayload = new byte[0];
			
			// Loop through the "rids" vector and build the "metaPayload" array
			for (int i = 0; i < rids.size(); i++) {
				
				// Get the current RID at index i
				RID rid = rids.get(i);
				
				// Get the chunk handle and the slot ID of the current RID
				byte[] handle = rid.getChunkHandle().getBytes();
				byte[] slot = ByteBuffer.allocate(SlotSize).putInt(rid.getSlotID()).array();
				
				// Reallocate the length of the "metaPayload" to accommodate the current RID
				metaPayload = new byte[tempPayload.length + handle.length + slot.length];
				
				// Copy the old "tempPayload", current "handle", and current "slot"
				System.arraycopy(tempPayload, 0, metaPayload, 0, tempPayload.length);
				System.arraycopy(handle, 0, metaPayload, tempPayload.length, handle.length);
				System.arraycopy(slot, 0, metaPayload, tempPayload.length + handle.length, slot.length);
				
				// Get a new copy of the "metaPayload"
				tempPayload = Arrays.copyOf(metaPayload, metaPayload.length);
			}
			
			// Calculate the needed space and number of needed chunks for the "metaPayload"
			long metaNeededSpace = MetaByteSize + SubByteSize + LengthSize + metaPayload.length + SlotSize;
			int numMeta = (int)Math.ceil((double)metaNeededSpace / MaxNonHeaderSize);
			
			// Decide whether the "metaPayload" is big
			boolean bigMetaRecord = false;
			if (numMeta > 1) {
				bigMetaRecord = true;
			}
			
			// Break down the "metaPayload" into a vector of small byte arrays
			Vector<byte[]> subMetaPayloads = new Vector<byte[]>();
			for (int i = 0; i < numMeta; i++) {
				
				// Calculate the start and end index of the smaller chunks
				int startIndex = i * MaxRawPayloadSize;
				int endIndex = (i+1) * MaxRawPayloadSize;
				
				// If this is the last smaller chunks
				if (i == (numMeta - 1)) {
					endIndex = metaPayload.length;
				}
				
				// Copy this smaller chunks into the "subMetaPayloads" vector
				subMetaPayloads.add(Arrays.copyOfRange(metaPayload, startIndex, endIndex));
			}
			
			// Clear the "rids" vector
			rids.clear();
			
			// Append the "subMetaPayloads" vector into the chunks
			for (int i = 0; i < subMetaPayloads.size(); i++) {
				
				// Get the current effective (smaller) metaPayload
				byte[] effMetaPayload = subMetaPayloads.get(i);
				
				boolean success = false;
				while (!success) {
					// Get the last chunk handle of the file handle
					String lastHandle = ofh.getChunkHandles().lastElement();
					String effHandle = lastHandle;
					
					// Read the header info 
					ByteBuffer header = ByteBuffer.wrap(cs.readChunk(effHandle, 0, ChunkServer.HeaderSize));
					int numRec = header.getInt();
					int offset = header.getInt();
					int firstSlot = header.getInt();
					int lastSlot = header.getInt();
					
					// Calculate the needed space for this smaller metaPayload
					metaNeededSpace = MetaByteSize + SubByteSize + LengthSize + effMetaPayload.length + SlotSize;
					int freeSpace = (slotIDToSlotOffset(lastSlot)) - offset;
					
					// If this smaller metaPaylaod can fit into this chunk
					if (metaNeededSpace <= freeSpace) {
						
						// Decide the "subType"
						byte subType = Entire;
						if (bigMetaRecord) {
							subType = Sub;
						}
						
						// Write this smaller metaPayload into this chunk
						rids.add(writeToChunk(effMetaPayload, numRec, offset, effHandle, lastHandle, Meta, subType, lastSlot, firstSlot));
						success = true;
					}
					
					// Cannot fit in to this chunk
					else {
						
						// Pad the rest of this chunk
						offset = slotIDToSlotOffset(lastSlot);
						
						// Open a chunk
						byte[] headerInfo = ByteBuffer.allocate(4).putInt(offset).array();
						cs.writeChunk(effHandle, headerInfo, 4);
						master.AddChunk(ofh.getFilePath());
						master.OpenFile(ofh.getFilePath(), ofh);
					}
				}
			}
		}
		
		// Get the last RID
		RID lastRID = rids.lastElement();
		RecordID.setChunkHandle(lastRID.getChunkHandle());
		RecordID.setSlotID(lastRID.getSlotID());
		
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
		if (ofh.getFilePath() == null)
			return ClientFS.FSReturnVals.BadHandle;
		
		if (RecordID == null) {
			return ClientFS.FSReturnVals.BadRecID;
		}
		// Check if given chunkHandle exist in the file
		Vector<String> chunkHandles = ofh.getChunkHandles();
		String chunkHandle = RecordID.getChunkHandle();
		if (!chunkHandles.contains(chunkHandle))
			return ClientFS.FSReturnVals.BadHandle;
		
		int slotID = RecordID.getSlotID();
		
		// Read the chunk header
		ByteBuffer header = ByteBuffer.wrap(cs.readChunk(chunkHandle, 0, ChunkServer.HeaderSize));
		int numRec = header.getInt();
		int freeOffset = header.getInt();
		int firstRec = header.getInt();
		int lastRec = header.getInt();
		
		// if invalid chunk or slotID
		if (numRec == 0 || slotID < firstRec || slotID > lastRec)
			return ClientFS.FSReturnVals.RecDoesNotExist;
		
		// Read the content at slotID
		int recOffset = ByteBuffer.wrap(cs.readChunk(chunkHandle, slotIDToSlotOffset(slotID), SlotSize)).getInt();
		// If record has been deleted
		if (recOffset == SlotNullified)
			return ClientFS.FSReturnVals.RecDoesNotExist;
		// Else delete the record
		else {
			// Invalidate the current slot
			recOffset = SlotNullified;
			cs.writeChunk(chunkHandle, ByteBuffer.allocate(SlotSize).putInt(recOffset).array(), slotIDToSlotOffset(slotID));
			
			// Update the header
			numRec--;
			
			// Move the first / last slot pointer accordingly
			if (slotID == firstRec) {
				// Move the first slot id to the next valid slotID
				firstRec++;
				while (firstRec <= lastRec) {
					int candidateContent = ByteBuffer.wrap(cs.readChunk(chunkHandle, slotIDToSlotOffset(firstRec), SlotSize)).getInt();
					if (candidateContent != SlotNullified)
						break;
					firstRec++;
				}
			}
			else if (slotID == lastRec) {
				lastRec--;
				while (lastRec >= firstRec) {
					int candidateContent = ByteBuffer.wrap(cs.readChunk(chunkHandle, slotIDToSlotOffset(lastRec), SlotSize)).getInt();
					if (candidateContent != SlotNullified)
						break;
					lastRec--;
				}
			}
			if (firstRec > lastRec)
				// no valid record in the chunk?
				return ClientFS.FSReturnVals.Fail;
			
			updateChunkHeader(numRec, freeOffset, firstRec, lastRec, chunkHandle);
		}

		return ClientFS.FSReturnVals.Success;
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
		int index = 0;
		
		byte[] recPayload = new byte[0];
		while (index < chunkHandles.size()) {
			String chunkHandle = chunkHandles.get(index);
			ByteBuffer header = ByteBuffer.wrap(cs.readChunk(chunkHandle, 0, ChunkServer.HeaderSize));
			if (header == null)
				return ClientFS.FSReturnVals.RecDoesNotExist;
			// Read the number of records
			int numRec = header.getInt();
			
			// if nothing in the chunk, skip to next chunk
			if (numRec == 0) {
				index++;
				if (index >= chunkHandles.size())
					return ClientFS.FSReturnVals.RecDoesNotExist;
				chunkHandle = chunkHandles.get(index);
				continue;
			}
			// Read the next free offset/free slot
			int offset = header.getInt();
			// Read the first record offset
			int firstSlotID = header.getInt();
			// Red the last record offset
			int lastSlotID = header.getInt();
			
			int slotID = firstSlotID;
			while (slotID <= lastSlotID) {
				if (slotID == -1) {
					slotID++;
					continue;
				}
				ByteBuffer intro = ByteBuffer.wrap(cs.readChunk(chunkHandle, slotIDToSlotOffset(slotID), 4));
				int chunkloc = intro.getInt(); 
				ByteBuffer chunkdata = ByteBuffer.wrap(cs.readChunk(chunkHandle, chunkloc, 6));
				byte meta = chunkdata.get();
				byte sub = chunkdata.get();
				int length = chunkdata.getInt();
				
				// If the current record is a sub
				if(sub ==Sub && meta == Regular) {
					byte[] tempPayload = new byte[recPayload.length + length];
					byte[] currPayload = cs.readChunk(chunkHandle, chunkloc+6, length);
					
					System.arraycopy(recPayload, 0, tempPayload, 0, recPayload.length);
					System.arraycopy(currPayload, 0, tempPayload, recPayload.length, currPayload.length);
									
					recPayload = tempPayload;
				}
				else if (sub == Entire && meta == Meta) {
					RID newRID = new RID();
					newRID.setChunkHandle(chunkHandle);
					newRID.setSlotID(slotID);
					rec.setRID(newRID);
					rec.setPayload(recPayload);
					
					return ClientFS.FSReturnVals.Success;
				}
				else if(meta == Regular) {
					recPayload = cs.readChunk(chunkHandle, chunkloc+6, length);
					RID newRID = new RID();
					newRID.setChunkHandle(chunkHandle);
					newRID.setSlotID(firstSlotID);
					rec.setRID(newRID);
					rec.setPayload(recPayload);
					
					return ClientFS.FSReturnVals.Success;
				}
				slotID++;
			}
		}
		
		return ClientFS.FSReturnVals.Fail;
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
		
		Vector<String> chunkHandles = ofh.getChunkHandles();
		int index = chunkHandles.size() - 1;
		
		byte[] recPayload = new byte[0];
		while (index >= 0) {
			String chunkHandle = chunkHandles.get(index);			
			ByteBuffer header = ByteBuffer.wrap(cs.readChunk(chunkHandle, 0, ChunkServer.HeaderSize));
			if (header == null)
				return ClientFS.FSReturnVals.RecDoesNotExist;
		// Read the number of records
			int numRec = header.getInt();
			
			if (numRec == 0) {
				index--;
				if (index < 0)
					return ClientFS.FSReturnVals.RecDoesNotExist;
				chunkHandle = chunkHandles.get(index);
				continue;
			}
			// Read the next free offset/free slot
			int offset = header.getInt();
			int firstSlotID = header.getInt();
			int lastSlotID = header.getInt();
			
			int slotID = lastSlotID;
			while (slotID >= firstSlotID) {
				if (slotID == -1) {
					slotID--;
					continue;
				}
				ByteBuffer lastRecSlot = ByteBuffer.wrap(cs.readChunk(chunkHandle, slotIDToSlotOffset(lastSlotID), SlotSize));
				int lastRecID = lastRecSlot.getInt();
				
				ByteBuffer chunkdata = ByteBuffer.wrap(cs.readChunk(chunkHandle, lastRecID, 6));
				byte meta = chunkdata.get();
				byte sub = chunkdata.get();
				int recLen = chunkdata.getInt();
				
				if(sub == Sub && meta == Regular) {
					byte[] tempPayload = new byte[recPayload.length + recLen];
					byte[] currPayload = cs.readChunk(chunkHandle, lastRecID+6, recLen);
					
					System.arraycopy(recPayload, 0, tempPayload, 0, recPayload.length);
					System.arraycopy(currPayload, 0, tempPayload, recPayload.length, currPayload.length);
									
					recPayload = tempPayload;
				}
				else if (sub == Entire && meta == Meta) {
					RID newRID = new RID();
					newRID.setChunkHandle(chunkHandle);
					newRID.setSlotID(slotID);
					rec.setRID(newRID);
					rec.setPayload(recPayload);
					
					return ClientFS.FSReturnVals.Success;
				}
				else if(meta == Regular && sub == Entire) {
					recPayload = cs.readChunk(chunkHandle, lastRecID+6, recLen);
					RID newRID = new RID();
					newRID.setChunkHandle(chunkHandle);
					newRID.setSlotID(lastSlotID);
					rec.setRID(newRID);
					rec.setPayload(recPayload);
			
					return ClientFS.FSReturnVals.Success;
				}
				slotID--;
			}
		}
		return ClientFS.FSReturnVals.Fail;
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */	
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec) {
		if (ofh == null)
			return ClientFS.FSReturnVals.BadHandle;
		
		// Get the index of the chunkHandle in the file
		String chunkHandle = pivot.getChunkHandle();
		int pivotSlotID = pivot.getSlotID();
		int nextSlotID = pivotSlotID + 1;
		Vector<String> chunkHandles = ofh.getChunkHandles();
		int ind = chunkHandles.indexOf(chunkHandle);
		
		// If chunkHandle is not a part of the file
		if (ind == -1)
			return ClientFS.FSReturnVals.BadHandle;
		
		// Check if current pivot is valid
		int pivotRecOffset = ByteBuffer.wrap(cs.readChunk(chunkHandle, slotIDToSlotOffset(pivotSlotID), SlotSize)).getInt();
		if (pivotRecOffset == SlotNullified) {
			return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		ByteBuffer pivotIntro = ByteBuffer.wrap(cs.readChunk(chunkHandle, pivotRecOffset, MetaByteSize + SubByteSize));
		if (pivotIntro == null) {
			return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		byte pivotMetaType = pivotIntro.get();
		byte pivotSubType = pivotIntro.get();
		
		// If the pivot is a sub record
		if (pivotSubType == Sub) {
			return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		
		byte[] recPayload = new byte[0];
		// Iterate until last chunkHandle of the file (unless it succeeds)
		while (ind < chunkHandles.size()) {
			chunkHandle = chunkHandles.get(ind);
			ByteBuffer header = ByteBuffer.wrap(cs.readChunk(chunkHandle, 0, ChunkServer.ChunkSize));
			// on 4/23/18 we discussed that our implementation would be to nullify records by setting slotID = -1
			if (header == null || pivotSlotID == -1)
				return ClientFS.FSReturnVals.RecDoesNotExist;
			
			// Read the number of records
			int numRec = header.getInt();
			// Read the next free offset/free slot
			int offset = header.getInt();
			int firstSlotID = header.getInt();
			int lastSlotID = header.getInt();
			
			while (nextSlotID <= lastSlotID) {
				// if there are no more next records in the chunk
				if (numRec == 0) {
					break;
				}
				// if the next record within the chunk
				else if (nextSlotID <= lastSlotID) {
					if (nextSlotID < firstSlotID)
						nextSlotID = firstSlotID;
					ByteBuffer nextSlot = ByteBuffer.wrap(cs.readChunk(chunkHandle, slotIDToSlotOffset(nextSlotID), 4));
					int nextRecOffset = nextSlot.getInt();
					
					// If nextRec is valid
					if (nextRecOffset != -1) {
						ByteBuffer nextRec = ByteBuffer.wrap(cs.readChunk(chunkHandle, nextRecOffset, 6));		
						byte meta = nextRec.get();
						byte sub = nextRec.get();
						int recLen = nextRec.getInt();
						if(sub ==Sub && meta == Regular) {
							byte[] tempPayload = new byte[recPayload.length + recLen];
							byte[] currPayload = cs.readChunk(chunkHandle, nextRecOffset+IntroSize, recLen);
							
							System.arraycopy(recPayload, 0, tempPayload, 0, recPayload.length);
							System.arraycopy(currPayload, 0, tempPayload, recPayload.length, currPayload.length);
											
							recPayload = tempPayload;
						}
						else if (sub == Entire && meta == Meta) {
							RID newRID = new RID();
							newRID.setChunkHandle(chunkHandle);
							newRID.setSlotID(nextSlotID);
							rec.setRID(newRID);
							rec.setPayload(recPayload);
							
							return ClientFS.FSReturnVals.Success;
						}
						else if(meta == Regular && sub == Regular) {
							recPayload = cs.readChunk(chunkHandle, nextRecOffset+6, recLen);
							RID newRID = new RID();
							newRID.setChunkHandle(chunkHandle);
							newRID.setSlotID(nextSlotID);
							rec.setRID(newRID);
							rec.setPayload(recPayload);
							
							return ClientFS.FSReturnVals.Success;
						}
					}
					nextSlotID++;
				}
			}
			ind++;
			nextSlotID = 0;
		}
				
		return ClientFS.FSReturnVals.Fail;
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

		String chunkHandle = pivot.getChunkHandle();
		int pivotSlotID = pivot.getSlotID();
		int prevSlotID = pivotSlotID - 1;
		Vector <String> chunkHandles = ofh.getChunkHandles();
		int ind = chunkHandles.indexOf(chunkHandle);

		if (ind == -1)
			return ClientFS.FSReturnVals.BadHandle;
		
		// Check if current pivot is valid
		int pivotRecOffset = ByteBuffer.wrap(cs.readChunk(chunkHandle, slotIDToSlotOffset(pivotSlotID), SlotSize)).getInt();
		if (pivotRecOffset == SlotNullified) {
			return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		ByteBuffer pivotIntro = ByteBuffer.wrap(cs.readChunk(chunkHandle, pivotRecOffset, MetaByteSize + SubByteSize));
		if (pivotIntro == null) {
			return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		byte pivotMetaType = pivotIntro.get();
		byte pivotSubType = pivotIntro.get();

		// If the pivot is a sub record
		if (pivotSubType == Sub) {
			return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		
		byte[] recPayload = new byte[0];
		while (ind >= 0) {
			chunkHandle = chunkHandles.get(ind);
			ByteBuffer header = ByteBuffer.wrap(cs.readChunk(chunkHandle, 0, ChunkServer.ChunkSize));
			// on 4/23/18 we discussed that our implementation would be to nullify records by setting slotID = -1
			if (header == null || pivotSlotID == -1)
				return ClientFS.FSReturnVals.RecDoesNotExist;
			
			// Read the number of records
			int numRec = header.getInt();
			// Read the next free offset/free slot
			int offset = header.getInt();
			int firstSlotID = header.getInt();
			int lastSlotID = header.getInt();
			
			while (prevSlotID >= firstSlotID) {
				// if there are no more prev records in the chunk
				if (numRec == 0) {
					break;
				}
				// if the next record within the chunk
				else if (prevSlotID >= firstSlotID) {
					if (prevSlotID > lastSlotID)
						prevSlotID = lastSlotID;
					ByteBuffer prevSlot = ByteBuffer.wrap(cs.readChunk(chunkHandle, slotIDToSlotOffset(prevSlotID), 4));
					int prevRecOffset = prevSlot.getInt();
					
					if (prevRecOffset != -1) {
						ByteBuffer prevRec = ByteBuffer.wrap(cs.readChunk(chunkHandle, prevRecOffset, 6));		
						byte meta = prevRec.get();
						byte sub = prevRec.get();
						int recLen = prevRec.getInt();
						if(sub ==Sub && meta == Regular) {
							byte[] tempPayload = new byte[recPayload.length + recLen];
							byte[] currPayload = cs.readChunk(chunkHandle, prevRecOffset+IntroSize, recLen);
							
							System.arraycopy(recPayload, 0, tempPayload, 0, recPayload.length);
							System.arraycopy(currPayload, 0, tempPayload, recPayload.length, currPayload.length);
											
							recPayload = tempPayload;
						}
						else if (sub == Entire && meta == Meta) {
							RID newRID = new RID();
							newRID.setChunkHandle(chunkHandle);
							newRID.setSlotID(prevSlotID);
							rec.setRID(newRID);
							rec.setPayload(recPayload);
							
							return ClientFS.FSReturnVals.Success;
						}
						else if(meta == Regular && sub == Entire) {
							recPayload = cs.readChunk(chunkHandle, prevRecOffset+6, recLen);
							RID newRID = new RID();
							newRID.setChunkHandle(chunkHandle);
							newRID.setSlotID(prevSlotID);
							rec.setRID(newRID);
							rec.setPayload(recPayload);
							
							return ClientFS.FSReturnVals.Success;
						}
					}
					prevSlotID--;
				}
			}
			ind--;
			prevSlotID = Integer.MAX_VALUE;
		}
				
		return ClientFS.FSReturnVals.RecDoesNotExist;
		
		
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

		System.arraycopy(payloadSize, 0, effPayload, 2, payloadSize.length);
		System.arraycopy(payload, 0, effPayload, 2+payloadSize.length, payload.length);
		
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
	
	/**
	 * Populates the payload byte array with the extracted payload
	 * Returns the type of the record
	 * 0 = regular, entire
	 * 1 = regular, sub
	 * 2 = meta, entire
	 * 3 = meta, sub
	 * @param chunkHandle
	 * @param slotID
	 * @param payload
	 */
	public int getPayloadFromSlotID(String chunkHandle, int slotID, byte[] payload) {
		ByteBuffer offsetBuf = ByteBuffer.wrap(cs.readChunk(chunkHandle, slotIDToSlotOffset(slotID), 4));
		int recOffset = offsetBuf.getInt();
		
		// 6 bytes for non-payload in the record
		ByteBuffer recBuf = ByteBuffer.wrap(cs.readChunk(chunkHandle, recOffset, 6));
		byte meta = recBuf.get();
		byte sub = recBuf.get();
		int length = recBuf.getInt();
		
		// populate the payload
		payload = cs.readChunk(chunkHandle, recOffset + 6, length);
		
		if (meta == Meta) {
			if (sub == Sub)
				return 3;
			return 2;
		}
		else {
			if (sub == Sub)
				return 1;
			return 0;
		}
	}

}
