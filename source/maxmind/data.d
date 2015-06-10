module maxmind.data;
import std.bitmanip : bigEndianToNative;
import std.conv : to;

/**
 * Generic container class that represents a metadata node for the Maxmind database format.
 */
public abstract class DataNode {
	enum Type : ubyte {
		Extended = 0,
		Pointer  = 1,
		String   = 2,
		Double   = 3,
		Binary   = 4,
		Uint16   = 5,
		Uint32   = 6,
		Int32    = 8,
		Uint64   = 9,
		Uint128  = 10,
		Map      = 7,
		Array    = 11,
		CacheContainer = 12,
		EndMarker = 13,
		Boolean  = 14,
		Float    = 15
	}
	
	/** Holds the original type of the data */
	protected Type  _type;
	
	/** Constructs the node with the appropriate type tag */
	public this(Type type) {
		this._type = type;
	}
	
	/** Readonly accessor for the node type */
	@property public Type type() const {
		return this._type;
	}
	
	
	/**
	 * Creates a new node of the appropriate type using an existing reader helper as the data source
	 */
	public static DataNode create(ref Reader data) {
		byte id = data.read();
		
		// Read object type
		Type type = cast(Type)((id & 0b11100000) >> 5);
		
		if(type == Type.Extended) {
			type = cast(Type)(data.read() + 7);
		}
		
		// Read payload size
		ulong payloadSize = id & 0b00011111;
		
		if(payloadSize >= 29) final switch(payloadSize) {
			case 29: payloadSize = 29    + bigEndianToNative!uint(data.read!4(1)); break;
			case 30: payloadSize = 285   + bigEndianToNative!uint(data.read!4(2)); break;
			case 31: payloadSize = 65821 + bigEndianToNative!uint(data.read!4(3)); break;
		}
		
		// Construct the object node
		switch(type) {
			case Type.Pointer:        assert(false, "Unimplemented type: Pointer");
			case Type.String:         return new String        (type, data, payloadSize);
			case Type.Double:         return new Number!double (type, data, payloadSize);
			case Type.Binary:         return new Binary        (type, data, payloadSize);
			case Type.Uint16:         return new Number!ushort (type, data, payloadSize);
			case Type.Uint32:         return new Number!uint   (type, data, payloadSize);
			case Type.Map:            return new Map           (type, data, payloadSize);
			case Type.Int32:          return new Number!int    (type, data, payloadSize);
			case Type.Uint64:         return new Number!ulong  (type, data, payloadSize);
			case Type.Uint128:        return new Binary        (type, data, payloadSize);
			case Type.Array:          return new Array         (type, data, payloadSize);
			case Type.CacheContainer: assert(false, "Unimplemented type: CacheContainer");
			case Type.EndMarker:      assert(false, "Unimplemented type: EndMarker");
			case Type.Boolean:        return new Boolean       (type, data, payloadSize);
			case Type.Float:          return new Number!float  (type, data, payloadSize);
			default:
				return new Binary(type, data, payloadSize);
		}
	}
	
	/**
	 * Creates a new node using a raw data slice with an optional offset to the beginning of the data section.
	 * @see create(ref Reader data)
	 */
	public static DataNode create(ubyte[] data, ulong offset = 0) {
		Reader reader = Reader(data, offset);
		return DataNode.create(reader);
	}
	

	
	/**
	 * Generic binary data type
	 */
	public static class Binary : DataNode {
		protected ubyte[] _data;
		
		@property const(ubyte[]) data() const {
			return this._data;
		}
		
		public this(Type type, ref Reader data, ulong payloadSize) {
			super(type);
			this._data = data.read(payloadSize);
		}
	}
	
	
	/**
	 * Holds strings coming from the database
	 */
	public static class String : DataNode {
		protected string _value;
		
		public this(Type type, ref Reader data, ulong length) {
			super(type);
			this._value = cast(string) data.read(length);
		}
		
		@property public string value() {
			return this._value;
		}
	}
	
	
	/**
	 * Holds any kind of number from the database
	 */
	public static class Number(T) : DataNode {
		protected T _value;
		
		public this(Type type, ref Reader data, ulong actualSize) {
			super(type);
			this._value = bigEndianToNative!T(data.read!(T.sizeof)(actualSize));
		}
		
		@property public T value() {
			return this._value;
		}
	}
	
	
	/**
	 * Holds a boolean value from the database
	 */
	public static class Boolean : DataNode {
		protected bool _value;
		
		public this(Type type, ref Reader data, ulong value) {
			super(type);
			this._value = value > 0;
		}
		
		@property public bool value() {
			return this._value;
		}
	}
	
	
	/**
	 * Holds a Map structure from the database. 
	 */
	public static class Map : DataNode {
		DataNode[string] _map;
		
		
		/**
		 * Constructs the map and loads all the key/value pairs
		 */
		public this(Type type, ref Reader data, ulong numValues) {
			super(type);
			
			while(numValues > 0) {
				DataNode key   = DataNode.create(data);
				DataNode value = DataNode.create(data);
				
				if(key.type != Type.String) {
					throw new Exception("Invalid map key: expected string, got " ~ key.type.to!string ~ " instead.");
				}
				
				DataNode.String keyString = cast(DataNode.String) key;
				this._map[keyString.value] = value;
				numValues--;
			}
		}
		
		
		/** Allow using foreach over the map's keys and values */
		public int opApply(int delegate(string, DataNode) dg) {
			int result = 0;
			
			foreach(key, node; this._map) {
				result = dg(key, node);
				if(result) return result;
			}
			
			return 0;
		}
		
		/** Allow accessing the map as an associative array */
		public DataNode opIndex(string key) {
			return this._map[key];
		}
		
		/** Allow accessing the map as if values were direct sub-objects */
		public DataNode opDispatch(string key)() {
			return this[key];
		}
	}
	
	
	/**
	 * Holds an Array structure from the database.
	 */
	public static class Array : DataNode {
		DataNode[] _values;
		
		/**
		 * Constructs the Array as well as all its contained values
		 */
		public this(Type type, ref Reader data, ulong length) {
			super(type);
			
			while(length > 0) {
				this._values ~= DataNode.create(data);
				length--;
			}
		}
		
		
		/** Returns the length of the array */
		@property public ulong length() {
			return this._values.length;
		}
		
		
		/** Allow using foreach over the array of nodes */
		public int opApply(int delegate(ulong, DataNode) dg) {
			int result = 0;
			
			foreach(key, node; this._values) {
				result = dg(key, node);
				if(result) return result;
			}
			
			return 0;
		}
		
		/** Allow accessing values from the array directly */
		public DataNode opIndex(ulong key) {
			return this._values[key];
		}
	}
	
	
	/**
	 * Accesses the node values directly by automatically casting the node and getting its value
	 */
	public T get(T)() {
		CastOutput conv(CastOutput)() {
			CastOutput output = cast(CastOutput)(this);
			
			if(output is null) {
				throw new Exception("Invalid conversion");
			}
			
			return output;
		}
		
		     static if(is(T == string))  return conv!String.value;
		else static if(is(T == double))  return conv!(Number!double);
		else static if(is(T == ubyte[])) return conv!Binary.data;
		else static if(is(T == ushort))  return conv!(Number!ushort).value;
		else static if(is(T == uint))    return conv!(Number!uint).value;
		else static if(is(T == int))     return conv!(Number!int).value;
		else static if(is(T == ulong))   return conv!(Number!ulong).value;
		else static if(is(T == bool))    return conv!Boolean.value;
		else static if(is(T == float))   return conv!(Number!float).value;
		else {
			static assert(false, "Unknown output conversion");
		}
	}
}


/**
 * Helper container that manages changing position in the mmapped database file. It automatically handles
 * moving the internal pointer forward when data is read to ensure proper alignment of data.
 */
package struct Reader {
	/** The raw slice of the whole database */
	ubyte[] data;
	
	/** Pointer to the current offset in the database file */
	ulong   current;
	
	
	/**
	 * Constructs a new reader from a slice and a starting offset
	 */
	this(ubyte[] data, ulong startOffset = 0) {
		this.data = data;
		this.current = startOffset;
	}
	
	
	/**
	 * Creates a new reader at a different offset: used by the pointer and cache types to go fetch data
	 * outside of their own location.
	 */
	Reader newReader(ulong offset) {
		return Reader(this.data, offset);
	}
	
	
	/**
	 * Reads $length bytes from the database
	 */
	ubyte[] read(ulong length) {
		ulong start = this.current;
		this.current += length;
		return this.data[start..this.current];
	}
	
	
	/**
	 * Reads $length bytes from the database into a fixed-size array of $outbytes.
	 */
	ubyte[outbytes] read(int outbytes)(ulong length) {
		ubyte[outbytes] output = 0;
		
		if(length > 0) {
			output[outbytes-length..outbytes] = this.read(length);
		}
		
		return output;
	}
	
	
	/**
	 * Reads a single byte from the database
	 */
	ubyte read() {
		return this.data[this.current++];
	}
}
