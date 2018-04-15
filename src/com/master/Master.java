package com.master;

import com.client.ClientFS;
import com.client.FileHandle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import com.client.ClientFS.FSReturnVals;

import javafx.util.Pair;

public class Master {
	
	// HashMap<'full folder path w/ trailing '/'', 'set of files/folders in it'>
	private static HashMap<String, HashSet<String>> directories;
	
	// HashMap<'full file path', 'sequence of chunkhandles'>
	private static HashMap<String, Vector<String>> files;
	
	// HashMap<'chunkhandle', Vector<Pair<'IP addr', 'alias'>>>
	// Alias is the local chunkhandle that may be different than the chunkhandle stored on Master
	private static HashMap<String, Vector<Pair<String, String>>> chunkLocations;
	
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
		
		// Check if src dir exists
		if (!directories.containsKey(src))
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		
		// Check if dirname exits
		if (directories.get(src).contains(src+dirname))
			return ClientFS.FSReturnVals.DestDirExists;
		
		// Else add the folder as a directories entry
		directories.put(src+dirname+'/', new HashSet<String>());
		// Add the folder in the set of its parents content
		directories.get(src).add(src+dirname);
		
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
		return null;
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
		return null;
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		return null;
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
		directories.get(tgtdir).add(tgtdir+filename);
		
		// TODO: Tell the chunkserver(s) to create a new chunk?
		
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		if (!directories.containsKey(tgtdir))
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		
		if (!files.containsKey(tgtdir+filename))
			return ClientFS.FSReturnVals.FileDoesNotExist;
		
		// Remove it from the set of files in the tgtdir
		directories.get(tgtdir).remove(tgtdir+filename);
		
		// Remove it from the file entries
		Vector<String> chunks = files.get(tgtdir+filename);
		files.remove(tgtdir+filename);
		
		// Remove chunks from chunkLocations
		for (String chunkHandle : chunks) {
			Vector<Pair<String, String>> locations = chunkLocations.get(chunkHandle);
			chunkLocations.remove(chunkHandle);
			
			// TODO: remove the chunks from the chunkserver(s)?
		}
		
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
		HashMap<String, Vector<Pair<String, String>>> locations = new HashMap<String, Vector<Pair<String, String>>>();
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
