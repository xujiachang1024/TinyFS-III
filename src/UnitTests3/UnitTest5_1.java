package UnitTests3;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.chunkserver.ChunkServer;
import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;
import com.master.Master;
import com.client.ClientRec;
import com.client.FileHandle;
import com.client.RID;
import com.client.TinyRec;

/**
 * UnitTest5 for Part 3 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */
public class UnitTest5_1 {
	
	public static int NumRecs = 1000;
	static final String TestName = "Unit Test 5.1: ";
	
	public static void main(String[] args) {
		// Create temporary master and chunkserver to be shared throughout the testing
		ChunkServer cs = new ChunkServer();
		Master master = new Master(cs);
				
		System.out.println(TestName + "Same as Unit Test 4 except that it manipulates the records starting with the last record, going backwards, and delete the even numbered records using their first four bytes.");
		String dir1 = "Shahram";
		ClientFS cfs = new ClientFS(master);
		FSReturnVals fsrv;
		
		//get the file handle first
		FileHandle fh = new FileHandle();
		FSReturnVals ofd = cfs.OpenFile("/" + dir1 + "/emp1", fh);
		byte[] payload = null;
		int intSize = Integer.SIZE / Byte.SIZE;	// 4 bytes
		ClientRec crec = new ClientRec(master,cs);
		
		ofd = cfs.OpenFile("/" + dir1 + "/emp1", fh);
		TinyRec r1 = new TinyRec();
		FSReturnVals retRR = crec.ReadLastRecord(fh, r1);
		while (r1.getRID() != null){
			TinyRec r2 = new TinyRec();
			FSReturnVals retval = crec.ReadPrevRecord(fh, r1.getRID(), r2);
			if(r2.getRID() != null){
				byte[] head = new byte[4];
				System.arraycopy(r2.getPayload(), 0, head, 0, 4);
				int value = ((head[0] & 0xFF) << 24) | ((head[1] & 0xFF) << 16)
				        | ((head[2] & 0xFF) << 8) | (head[3] & 0xFF);
				if(value % 2 == 0){
					System.out.println("Unit test 5 result: fail!  Found an even numbered record with value " + value + ".");
		    		return;
				}
				r1 = r2;
			}else{
				r1.setRID(null);
			}
		}
		fsrv = cfs.CloseFile(fh);
		System.out.println(TestName + "Success!");
	}

}
