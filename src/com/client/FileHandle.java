package com.client;

import java.util.HashMap;
import java.util.Vector;

import javafx.util.Pair;

public class FileHandle {
	
	// List the sequence of chunkhandles that make up the file
	// Vector<'chunkHandle'>
	private Vector<String> chunkHandles;
	
	// HashMap<'chunkHandle', Vector<'ip addr'>>
	private HashMap<String, Vector<String>> chunkLocations;
	
	// Full FilePath
	private String filePath;
	
	public FileHandle() {
		this.chunkHandles = new Vector<String>();
		this.chunkLocations = new HashMap<String, Vector<String>>();
	}
	
	public Vector<String> getChunkHandles() {
		return this.chunkHandles;
	}
	
	public void setChunkHandles(Vector<String> chunkHandles) {
		this.chunkHandles = chunkHandles;
	}
	
	public HashMap<String, Vector<String>> getChunkLocations() {
		return this.chunkLocations;
	}
	
	public void setChunkLocations(HashMap<String, Vector<String>> chunkLocations) {
		this.chunkLocations = chunkLocations;
	}
	
	public String getFilePath() {
		return this.filePath;
	}
	
	public void setFilePath(String FilePath) {
		this.filePath = filePath;
	}
}
