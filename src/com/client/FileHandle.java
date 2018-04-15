package com.client;

import java.util.HashMap;
import java.util.Vector;

import javafx.util.Pair;

public class FileHandle {
	
	// List the sequence of chunkhandles that make up the file
	// Vector<'chunkHandles'>
	private Vector<String> chunkHandles;
	
	// Stores chunkhandles as keys
	// Vector of all the locations (chunkservers' IP) that hold that chunk and its "alias"
	// on that specific chunkserver
	// HashMap<'chunkHandle', Vector<Pair<'IP addr', 'alias'>>>
	private HashMap<String, Vector<Pair<String, String>>> chunkLocations;
	
	public FileHandle() {
		this.chunkHandles = new Vector<String>();
		this.chunkLocations = new HashMap<String, Vector<Pair<String,String>>>();
	}
	
	public Vector<String> getChunkHandles() {
		return this.chunkHandles;
	}
	
	public void setChunkHandle(Vector<String> chunkHandles) {
		this.chunkHandles = chunkHandles;
	}
	
	public HashMap<String, Vector<Pair<String, String>>> getChunkLocations() {
		return this.chunkLocations;
	}
	
	public void setChunkLocations(HashMap<String, Vector<Pair<String, String>>> chunkLocations) {
		this.chunkLocations = chunkLocations;
	}
}
