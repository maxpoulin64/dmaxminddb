module maxmind.db;
public import maxmind.data;
import std.mmfile;

import std.stdio;

/** Constant byte sequence that marks the beginning of the global metadata section */
protected const ubyte[] METADATA_MARKER = [0xAB, 0xCD, 0xEF, 'M', 'a', 'x', 'M', 'i', 'n', 'd', '.', 'c', 'o', 'm'];

/** Maximum size of the global metadata section at the end of the file */
protected const ulong   METADATA_MAX_SIZE = 128*1024;


/**
 * Holds the standard global database metadata
 */
public struct Metadata {
	uint           node_count;
	ushort         record_size;
	ushort         ip_version;
	string         database_type;
	string[string] languages;
	ushort         binary_format_major_version;
	ushort         binary_format_minor_version;
	
	
	public this(in DataNode node) {
		
	}
}


/**
 * Handles opening, reading and accessing the MaxMind GeoIP database
 */
class Database {
	/** The memory-mapped database file for easy and fast access */
	protected MmFile dbfile;
	
	/**
	 * Opens the database located at $filename on the system.
	 */
	public this(string filename) {
		this.dbfile = new MmFile(filename);
		this.readMetadata();
	}
	
	
	/**
	 * Reads the database metadata from the file and sets it in the instance
	 */
	protected void readMetadata() {
		ulong metadata_start = this.findMetadataBlockStart();
		DataNode.Map metadata_node = cast(DataNode.Map) DataNode.create(
			cast(ubyte[]) this.dbfile[metadata_start..this.dbfile.length]
		);
		
		writeln("node count is: " ~ metadata_node.node_count.get!uint.to!string);
	}
	
	
	/**
	 * Scans the database file from the end to locate the metadata block
	 * (Yes, this is how MaxMind says to do)
	 */
	protected ulong findMetadataBlockStart() {
		ulong position = this.dbfile.length - METADATA_MARKER.length - 1;
		ulong minposition = // Don't attempt reading before the beginning of the file
			this.dbfile.length > METADATA_MAX_SIZE ?
			this.dbfile.length - METADATA_MAX_SIZE : 0;
		
		// Scan the file from the end
		while(position > minposition) {
			if(this.dbfile[position..position+METADATA_MARKER.length] == METADATA_MARKER) {
				return position + METADATA_MARKER.length;
			}
			else {
				position--;
			}
		}
		
		// Metadata not found: database is unusable.
		throw new Exception(
			"Cannot find the metadata marker. This file doesn't appear to be a valid MaxMind v2 database."
		);
	}
}
