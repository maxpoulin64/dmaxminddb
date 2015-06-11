module maxmind.db;
public import maxmind.data;
import std.algorithm.mutation;
import std.algorithm.searching;
import std.conv : to;
import std.mmfile;
import std.regex;
import std.string;

/** Constant byte sequence that marks the beginning of the global metadata section */
protected const ubyte[] METADATA_MARKER = [0xAB, 0xCD, 0xEF, 'M', 'a', 'x', 'M', 'i', 'n', 'd', '.', 'c', 'o', 'm'];

/** Maximum size of the global metadata section at the end of the file */
protected const ulong   METADATA_MAX_SIZE = 128*1024;

/** The number of padding bytes between the search tree and the data section itself */
protected const ulong   DATA_SEPARATOR = 16;


/**
 * Holds the standard global database metadata
 */
public struct Metadata {
	uint           node_count;
	ushort         record_size;
	ushort         ip_version;
	string         database_type;
	string[]       languages;
	ushort         binary_format_major_version;
	ushort         binary_format_minor_version;
	
	
	public this(DataNode.Map m) {
		this.node_count    = m.node_count.get!uint;
		this.record_size   = m.record_size.get!ushort;
		this.ip_version    = m.ip_version.get!ushort;
		this.database_type = m.database_type.get!string;
		this.languages     = m.languages.getArray!string;
		
		this.binary_format_major_version = m.binary_format_major_version.get!ushort;
		this.binary_format_minor_version = m.binary_format_minor_version.get!ushort;
	}
	
	@property public ulong node_size() const {
		return this.record_size / 4;
	}
	
	@property public ulong data_size() const {
		return this.node_size * this.node_count;
	}
}


/**
 * Handles opening, reading and accessing the MaxMind GeoIP database
 */
class Database {
	/** The memory-mapped database file for easy and fast access */
	protected MmFile   dbfile;
	protected Metadata _metadata;
	protected Reader   dataReader;
	
	/**
	 * Opens the database located at $filename on the system.
	 */
	public this(string filename) {
		this.dbfile = new MmFile(filename);
		this.readMetadata();
		
		this.dataReader = Reader(
			cast(ubyte[]) this.dbfile[this._metadata.data_size+DATA_SEPARATOR .. this.dbfile.length]
		);
	}
	
	/**
	 * Read-only accessor for the metadata
	 */
	@property public const(Metadata) metadata() const {
		return this._metadata;
	}
	
	
	/**
	 * Reads the database metadata from the file and sets it in the instance
	 */
	protected void readMetadata() {
		ulong metadata_start = this.findMetadataBlockStart();
		DataNode.Map metadata_node = cast(DataNode.Map) DataNode.create(
			cast(ubyte[]) this.dbfile[metadata_start .. this.dbfile.length]
		);
		
		this._metadata = Metadata(metadata_node.asMap());
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
	
	
	/**
	 * Looks up any IP address in the database by delegating the search to the appropriate function
	 */
	public DataNode lookup(string address) {
		return address.canFind(':') ? this.lookupV6(address) : this.lookupV4(address);
	}
	
	
	/**
	 * Looks up an IPv4 address in the database, or return null if the record is not found.
	 */
	public DataNode lookupV4(string address) {
		if(!address.matchFirst(ctRegex!("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$"))) {
			throw new Exception("Invalid IPv4 address format");
		}
		
		string[4] parts = address.split('.');
		return this.lookupV4(parts.to!(ubyte[4]));
	}
	
	/// ditto
	public DataNode lookupV4(ubyte[4] address) {
		if(this._metadata.ip_version == 4) {
			return this.lookup_impl(address);
		}
		else {
			ubyte[16] address6 = 0;
			address6[12..16] = address;
			return this.lookupV6(address6);
		}
	}
	
	
	/**
	 * Looks up an IPv6 address in the database, or return null if the record is not found.
	 */
	public DataNode lookupV6(string address) {
		return this.lookupV6(parseIPv6(address));
	}
	
	/// ditto
	public DataNode lookupV6(ubyte[16] address) {
		if(this._metadata.ip_version != 6) {
			throw new Exception("This database does not support IPv6 addresses.");
		}
		
		return this.lookup_impl(address);
	}
	
	
	/**
	 * Internal implementation of the database search algorithm: takes the address as an array and searches the tree.
	 */
	protected DataNode lookup_impl(ubyte[] address) {
		DatabaseNode node = this.getNodeAt(0);
		
		// Loop over each group
		foreach(group; address) {
			ubyte mask = 0b10000000;
			
			// Move the bit mask by one for each iteration
			while(mask) {
				uint next = (group & mask) ? node.right : node.left;
				
				// Link to another node
				if(next < this._metadata.node_count) {
					node = this.getNodeAt(next);
				}
				
				// Record not found special value
				else if(next == this._metadata.node_count) {
					return null;
				}
				
				// Found data in the data section, read it
				else {
					next -= this._metadata.node_count + DATA_SEPARATOR;
					return DataNode.create(this.dataReader.newReader(next));
				}
				
				mask >>= 1;
			}
		}
		
		return null;
	}
	
	
	/**
	 * Returns a database node at $position (in node count)
	 */
	protected DatabaseNode getNodeAt(ulong position) {
		ulong  node_size = this._metadata.node_size;
		ulong offset    = position * node_size;
		
		return new DatabaseNode(
			cast(ubyte[]) this.dbfile[offset..offset+node_size]
		);
	}
}

/**
 * Holds an IP search node of the binary tree
 */
protected class DatabaseNode {
	uint left;
	uint right;
	
	/** Loads the raw node data into a convenient structure */
	public this(ubyte[] node) {
		/** Helper function to read a slice of arbitrary length into an uint */
		uint readSlice(ubyte[] slice) {
			ubyte[4] value;
			value[$-slice.length..$] = slice;
			return bigEndianToNative!uint(value);
		}
		
		// Decode the variable-sized node
		switch(node.length) {
			case 6:
				left  = readSlice(node[0..3]);
				right = readSlice(node[3..6]);
				break;
			
			case 7:
				left  = readSlice(node[0..3]) + ((node[3] & 0b11110000) << 20);
				right = readSlice(node[4..7]) + ((node[3] & 0b00001111) << 24);
				break;
			
			case 8:
				left  = readSlice(node[0..4]);
				right = readSlice(node[4..8]);
				break;
			
			default:
				throw new Exception("Cannot decode a node of an invalid size. Nodes must be 24, 28 or 32 bits long.");
		}
	}
}


/**
 * Utility function to parse an IPv6 address
 */
public ubyte[16] parseIPv6(string address) {
	ubyte[16] output = 0;
	int current = 31;
	int split = -1;
	int groupCount = 0;
	int groupCurrent = 0;
	int colons = 0;
	
	/**
	 * Helper function to convert a hex char to a number
	 */
	ubyte hexToByte(char c) {
		if(c >= '0' && c <= '9') return cast(ubyte)(c-'0');
		if(c >= 'A' && c <= 'F') return cast(ubyte)(c-'A'+10);
		if(c >= 'a' && c <= 'f') return cast(ubyte)(c-'a'+10);
		else throw new Exception("Invalid IPv6 address: invalid hexadecimal character.");
	}
	
	// Loop over each character from the end
	foreach_reverse(char c; address) {
		// If  that's a colon, count it
		if(c == ':') {
			if(++colons > 2) {
				throw new Exception("Invalid IPv6 address: too many colons in a row.");
			}
			
			// A colon means a group is closed, so we align back to the next 2 byte boundary
			current -= (current+1) % 4; // Need +1 here because we 0-index
			
			// Keep track of the number of groups for validation
			groupCurrent = 0;
			if(++groupCount > 8) {
				throw new Exception("IPv6: Too many groups");
			}
			
			// If that's the second colon in a row, mark where it is
			if(colons == 2) {
				split = current;
			}
		}
		else {
			colons = 0; // Reset the counter
			if(++groupCurrent > 4) {
				throw new Exception("Invalid IPv6 address: too many characters in a group.");
			}
			
			if(current < 0) {
				throw new Exception("Invalid IPv6 address: input address is too long.");
			}
			
			output[current/2] |= hexToByte(c) << (current & 1 ? 0 : 4);
			current--;
		}
	}
	
	// Align group being worked on
	current -= ((current+1) % 4) - 1; // +1 - Align to upper bound so /2 works
	split   += 1;                     // +1 - Same as above
	
	// If we found a :: split, move the bytes to the right position and fill zeros
	if(split > 0) {
		copy(output[current/2..split/2], output[0..(split-current)/2]);
		output[(split-current)/2..split/2] = 0;
	}
	
	return output;
}
