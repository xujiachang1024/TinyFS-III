package com.master;

import com.chunkserver.ChunkServer;
import com.client.ClientFS;
import com.client.FileHandle;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.Vector;

import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;
import com.client.FileHandle;

public class Master {
	
	public static final String masterBackupFileName = "masterBackup";
	
	// HashMap<'full folder path w/ trailing '/'', 'set of files/folders in it'>
	private static HashMap<String, HashSet<String>> directories;
	
	// HashMap<'full file path', 'sequence of chunkhandles'>
	private static HashMap<String, Vector<String>> files;
	
	// HashMap<'chunkhandle', Vector<'ip addr'>>
	private static HashMap<String, Vector<String>> chunkLocations;
	
	// Temporary local ChunkServer
	private static ChunkServer cs;
	
	
	public Master() {
		
		// initialize in-memory data structure
		initializeMemory();	
		
		cs = new ChunkServer();
	}
	
	/**
	 * Initialize in-memory data structure for the master node
	 */
	private void initializeMemory() {
		// Create a new HashMap and populate it with '/'
		directories = new HashMap<String, HashSet<String>>();
		directories.put("/", new HashSet<String>());
		
		files = new HashMap<String, Vector<String>>();
		
		chunkLocations = new HashMap<String, Vector<String>>();
	}
	
	/**
	 * Save master backup to a designated file
	 */
	private void saveMasterBackup() {
		FileOutputStream fos = null;
		ObjectOutputStream oos = null;
		try {
			fos = new FileOutputStream(masterBackupFileName);
			oos = new ObjectOutputStream(fos);
			oos.writeObject(directories);
			oos.writeObject(files);
			oos.writeObject(chunkLocations);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (oos != null) {
				try {
					oos.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Read master backup from a designated file
	 */
	@SuppressWarnings("unchecked")
	private void readMasterBackup() {
		FileInputStream fis = null;
		ObjectInputStream ois = null;
		try {
			fis = new FileInputStream(masterBackupFileName);
			ois = new ObjectInputStream(fis);
			directories = (HashMap<String, HashSet<String>>)ois.readObject();
			files = (HashMap<String, Vector<String>>)ois.readObject();
			chunkLocations = (HashMap<String, Vector<String>>)ois.readObject();
		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (ClassNotFoundException cnfe) {
			cnfe.printStackTrace();
		} finally {
			if (ois != null) {
				try {
					ois.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) {
		// When adding a folder as the key, include the trailing '/' as a part of the key
		// When adding a folder as a content of its parent directory, don't include '/'
		// Hashmap <dirString, list of files/dirs)
		// check if src is a dir (ending '/')
		// see if src exist in hashmap if yes, add dirname to its list, and append src+dirname to key with an empty list
		
		// Enforce "src" to end with "/"
		if (!src.endsWith("/")) {
			src += "/";
		}
		
		// Create the full path of the destination directory (end with "/")
		if (!dirname.endsWith("/")) {
			dirname += "/";
		}
		
		String destDirFullPath = src + dirname;
		
		// Check if "src" directory exists
		if (!directories.containsKey(src)) {
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		}
		
		// Check if "destDirFullPath" exits
		if (directories.containsKey(destDirFullPath)) {
			return ClientFS.FSReturnVals.DestDirExists;
		}

		// Add the folder in the set of its parents content
		directories.get(src).add(dirname);
		
		// Add the folder as a directories entry
		directories.put(destDirFullPath, new HashSet<String>());
		
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		
		// Enforce "src" to end with "/"
		if (!src.endsWith("/")) {
			src += "/";
		}
		
		// Create the full path of the destination directory (end with "/")
		if (!dirname.endsWith("/")) {
			dirname += "/";
		}
		
		String destDirFullPath = src + dirname;
		
		// Check if "src" or "destDirFullPath" exists
		if (!directories.containsKey(src) || !directories.containsKey(destDirFullPath)) {
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		}
		
		// Check if "destFullPath" is empty
		if (directories.get(destDirFullPath).size() != 0) {
			return ClientFS.FSReturnVals.DirNotEmpty;
		}
		
		directories.get(src).remove(dirname);
		directories.remove(destDirFullPath);
		
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * Returns SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "/Shahram/CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String NewName) {
		
		// Enforce "src" to end with "/"
		if (!src.endsWith("/")) {
			src += "/";
		}
		
		// Enforce "NewName" to end with "/"
		if (!NewName.endsWith("/")) {
			NewName += "/";
		}
		
		// Retrieve the full path of the immediate parent directory
		String[] srcSteps = src.split("/");
		String parentPath = "";
		for (int i = 0; i < srcSteps.length - 1; i++) {
			parentPath += (srcSteps[i] + "/");
		}
		
		// Check if the parent path matches for old/new directory names
		if (!NewName.startsWith(parentPath)) {
			return ClientFS.FSReturnVals.Fail;
		}
		
		// Check if parent directory exists
		if (!directories.containsKey(parentPath)) {
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		}
		
		// Check if "src" directory exists
		if (!directories.containsKey(src)) {
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		}
		
		// Get the target dirname (the short name, not the full path)
		String[] newSteps = NewName.split("/");
		int renameLevel = newSteps.length - 1;
		String srcDirname = srcSteps[renameLevel]+"/";
		String newDirname = newSteps[renameLevel]+"/";
		
		// Update the name in the HashSet value in the "directories" HashMap
		directories.get(parentPath).remove(srcDirname);
		directories.get(parentPath).add(newDirname);
		
		// Update the keys in the "directories" HashMap using BFS
		RenameDirBFS(src, newDirname, renameLevel);
		
		return ClientFS.FSReturnVals.Success;
	}
	
	private void RenameDirBFS(String srcFullPath, String newNameShort, int renameLevel) {
		
		// Create a Queue containing "oldFullPath"
		Queue<String> oldFullPathQueue = new LinkedList<String>();
		
		// Put the "srcFullPath" into the "oldFullPathQueue"
		oldFullPathQueue.add(srcFullPath);
		
		// Start BFS
		while (oldFullPathQueue.peek() != null) {
			
			// Poll the head of the "oldFullPathQueue"
			String oldFullPath = oldFullPathQueue.poll();
			
			// Add all the sub-directories of the "oldFullPath" to the "oldFullPathQueue"
			Iterator<String> iterator = directories.get(oldFullPath).iterator();
			while (iterator.hasNext()) {
				// Build the full path of the next child
				String childFullPath = oldFullPath + iterator.next();
				// Check if the next child is a sub-directory
				if (childFullPath.endsWith("/")) {
					// Add the sub-directory to the "oldFullPathQueue"
					oldFullPathQueue.add(childFullPath);
				}
			}
			
			// Build the full Path for the renamed directory
			String[] oldSteps = oldFullPath.split("/");
			String renamedFullPath = "";
			for (int i = 0; i < oldSteps.length; i++) {
				if (i == renameLevel)
					renamedFullPath += newNameShort;
				else
					renamedFullPath += (oldSteps[i] + "/");
			}
			
			// Update the key in the "directories" HashMap
			directories.put(renamedFullPath, directories.get(oldFullPath));
			directories.remove(oldFullPath);
		}
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		
		// Enforce "tgt" to end with "/"
		if (!tgt.endsWith("/")) {
			tgt += "/";
		}
		
		// Check if "tgt" directory exists
		if (!directories.containsKey(tgt)) {
			// TODO: what shall I return
			return null;
		}
		
		// Check if "tgt" directory is empty
		if (directories.get(tgt).size() == 0) {
			return null;
		}
		
		// Create a String Vector to collect all the sub-directories
		Vector<String> subDirVector = new Vector<String>();
		
		// Start DFS on the "tgt" directory
		ListDirDFS(tgt, subDirVector);
		
		// Convert the String Vector to a String Array
		String[] subDirArray = subDirVector.toArray(new String[subDirVector.size()]);
		
		return subDirArray;
	}
	
	private void ListDirDFS(String currFullPath, Vector<String> subDirVector) {
		
		// Retrieve the sub-directories HashSet
		HashSet<String> subDirSet = directories.get(currFullPath);
		
		// Iterate the HashSet
		Iterator<String> iterator = subDirSet.iterator();
		while (iterator.hasNext()) {
			// Retrieve the next directory/file
			String nextFullPath = currFullPath + iterator.next();

			// If the "nextFullPath" is a directory, start a recursive call
			if (nextFullPath.endsWith("/")) {
				ListDirDFS(nextFullPath, subDirVector);
				nextFullPath = nextFullPath.substring(0, nextFullPath.length()-1);
			}
			// Add the "nextFullPath" to the Vector
			subDirVector.add(nextFullPath);
		}
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		// Check if the target directory exists
		if (!directories.containsKey(tgtdir))
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		
		// Check if the file already exists
		if (files.containsKey(tgtdir+filename))
			return ClientFS.FSReturnVals.FileExists;
		
		// If all good, add the full file path to the list of files
		files.put(tgtdir+filename, new Vector<String>());
		// Add this to the list of files under the target dir
		directories.get(tgtdir).add(filename);
		
		// Create a unique uuid for the chunk and store it in the files namespace
		UUID uuid = UUID.randomUUID();
		files.get(tgtdir+filename).add(uuid.toString());
		
		// TODO: use the UUID to tell the chunkserver(s) to create an empty initial empty chunk
		// If successful add the current chunkserver ip addr to chunk namespace
		// Create an empty Chunk with the header
		// Header : 8 bytes [ 1) 4 bytes = # of records, 2) 4 bytes = offset for the next free byte]
		byte[] header = new byte[8];
		byte[] numRec = ByteBuffer.allocate(4).putInt(0).array();
		byte[] offset = ByteBuffer.allocate(4).putInt(8).array();
		
		System.arraycopy(numRec, 0, header, 0, numRec.length);
		System.arraycopy(offset, 0, header, numRec.length, offset.length);
		
		if (cs.writeChunk(uuid.toString(), header, 0))
			return ClientFS.FSReturnVals.Success;
		
		// Else return failure
		return ClientFS.FSReturnVals.Fail;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	// It makes the file as 'hidden' (notated by the '$' in the beginning) to be garbage collected later
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		if (!directories.containsKey(tgtdir))
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		
		if (!files.containsKey(tgtdir+filename))
			return ClientFS.FSReturnVals.FileDoesNotExist;
		
		// Remove it from the set of files in the tgtdir so that it is not listed
		directories.get(tgtdir).remove(filename);
		
		// Rename the file but keep the chunks intact
		Vector<String> chunks = files.get(tgtdir+filename);
		files.remove(tgtdir+filename);
		// Rename the file to be '.filename'
		files.put("$"+tgtdir+filename, chunks);
		
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx", FH1)
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		// Check if the file exist
		if (!files.containsKey(FilePath))
			return ClientFS.FSReturnVals.FileDoesNotExist;
		
		// If it exists, populate the FileHandle
		ofh.setChunkHandles(files.get(FilePath));
		HashMap<String, Vector<String>> locations = new HashMap<String, Vector<String>>();
		for (String chunkHandle : ofh.getChunkHandles()) {
			locations.put(chunkHandle, chunkLocations.get(chunkHandle));
		}
		
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
//	public FSReturnVals CloseFile(FileHandle ofh) {
//		// Check if ofh valid
//		if (ofh == null)
//			return ClientFS.FSReturnVals.BadHandle;
//		
//		ofh = null;
//		
//		return ClientFS.FSReturnVals.Success;
//	}
}
