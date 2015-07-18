module maxmind.data;
import std.bitmanip : bigEndianToNative;
import std.conv : to;
import std.traits;

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
		byte   id = data.read();
		size_t payloadSize;
		
		// Read object type
		Type type = cast(Type)((id & 0b11100000) >> 5);
		
		if(type == Type.Pointer) {
			return DataNode.followPointer(id, data);
		}
		
		if(type == Type.Extended) {
			type = cast(Type)(data.read() + 7);
		}
		
		// Read payload size
		payloadSize = id & 0b00011111;
		
		if(payloadSize >= 29) final switch(payloadSize) {
			case 29: payloadSize = data.read!uint(1) + 29;    break;
			case 30: payloadSize = data.read!uint(2) + 285;   break;
			case 31: payloadSize = data.read!uint(3) + 65821; break;
		}
		
		// Construct the object node
		switch(type) {
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
	
	/// ditto
	public static DataNode create(Reader r) {
		return DataNode.create(r);
	}
	
	/**
	 * Creates a new node using a raw data slice with an optional offset to the beginning of the data section.
	 * @see create(ref Reader data)
	 */
	public static DataNode create(ubyte[] data, size_t offset = 0) {
		return DataNode.create(Reader(data, offset));
	}
	
	/**
	 * Decodes and follows a pointer type
	 */
	public static DataNode followPointer(ubyte id, ref Reader data) {
		uint extrabits = id & 0b00000111;
		size_t jump;
		
		final switch((id &0b00011000) >> 3) {
			case 0: jump = data.read!uint(1, extrabits) + 0;      break;
			case 1: jump = data.read!uint(2, extrabits) + 2048;   break;
			case 2: jump = data.read!uint(3, extrabits) + 526336; break;
			case 3: jump = data.read!uint(4);                     break;
		}
		
		return DataNode.create(data.newReader(jump));
	}
	
	
	/**
	 * Generic binary data type
	 */
	public static class Binary : DataNode {
		protected ubyte[] _data;
		
		@property const(ubyte[]) data() const {
			return this._data;
		}
		
		public this(Type type, ref Reader data, size_t payloadSize) {
			super(type);
			this._data = data.read(payloadSize);
		}
	}
	
	
	/**
	 * Holds strings coming from the database
	 */
	public static class String : DataNode {
		protected string _value;
		
		public this(Type type, ref Reader data, size_t length) {
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
		
		public this(Type type, ref Reader data, size_t actualSize) {
			super(type);
			this._value = data.read!T(actualSize);
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
		
		public this(Type type, ref Reader data, size_t value) {
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
		public this(Type type, ref Reader data, size_t numValues) {
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
	}
	
	
	/**
	 * Holds an Array structure from the database.
	 */
	public static class Array : DataNode {
		DataNode[] _values;
		
		/**
		 * Constructs the Array as well as all its contained values
		 */
		public this(Type type, ref Reader data, size_t length) {
			super(type);
			
			while(length > 0) {
				this._values ~= DataNode.create(data);
				length--;
			}
		}
		
		
		/** Returns the length of the array */
		@property public size_t length() {
			return this._values.length;
		}
		
		
		/** Allow using foreach over the array of nodes */
		public int opApply(int delegate(size_t, DataNode) dg) {
			int result = 0;
			
			foreach(key, node; this._values) {
				result = dg(key, node);
				if(result) return result;
			}
			
			return 0;
		}
	}
	
	
	/**
	 * Accesses the node values directly by automatically casting the node and getting its value
	 */
	public T get(T)() {
		CastOutput conv(CastOutput)() {
			CastOutput output = cast(CastOutput)(this);
			
			if(output is null) {
				throw new Exception("Invalid conversion from " ~ this.type.to!string ~ " to " ~ T.stringof);
			}
			
			return output;
		}
		
		     static if(is(T == string))  return conv!String.value;
		else static if(is(T == double))  return conv!(Number!double).value;
		else static if(is(T == ubyte[])) return conv!Binary.data;
		else static if(is(T == ushort))  return conv!(Number!ushort).value;
		else static if(is(T == uint))    return conv!(Number!uint).value;
		else static if(is(T == int))     return conv!(Number!int).value;
		else static if(is(T == ulong))   return conv!(Number!ulong).value;
		else static if(is(T == bool))    return conv!Boolean.value;
		else static if(is(T == float))   return conv!(Number!float).value;
		else {
			static assert(false, "Unknown output conversion to " ~ T.stringof);
		}
	}
	
	
	/**
	 * Attemps to convert the current node to a Map node
	 */
	public Map asMap() {
		Map m = cast(Map) this;
		
		if(m is null) {
			throw new Exception("Using a node of type " ~ this.type.to!string ~ " as a map.");
		}
		
		return m;
	}
	
	/**
	 * Provide a way to export an entire map as an associative array of the specified type
	 */
	public T[string] getMap(T)() {
		Map m = this.asMap();
		T[string] map;
		
		foreach(key, node; m._map) {
			map[key] = node.get!T;
		}
		
		return map;
	}
	
	
	/**
	 * Attempts to convert the current node to an Array node
	 */
	public Array asArray() {
		Array a = cast(Array) this;
		
		if(a is null) {
			throw new Exception("Using a node of type " ~ this.type.to!string ~ " as an array");
		}
		return a;
	}
		
	/**
	 * Provide a way to export the entire array as a specific type
	 */
	public T[] getArray(T)() {
		Array a = this.asArray();
		T[] arr = new T[a._values.length];
		
		for(size_t i = 0; i < arr.length; i++) {
			arr[i] = a._values[i].get!T;
		}
		
		return arr;
	}
	
	
	/**
	 * Allow accessing Map values using dot notation
	 * Note: I put the method here to avoid having to cast objets all over the place
	 */
	public DataNode opDispatch(string key)() {
		if(key !in this.asMap()._map) {
			return null;
		}
		
		return this.asMap()._map[key];
	}
	
	
	/**
	 * Allow accessing Map values using the [] operator using string keys
	 * Note: I put the method here to avoid having to cast objets all over the place
	 */
	public DataNode opIndex(string key) {
		if(key !in this.asMap()._map) {
			return null;
		}
		
		return this.asMap()._map[key];
	}
	
	
	/**
	 * Allow accessing Array values using the [] operator using ulong keys
	 * Note: I put the method here to avoid having to cast objets all over the place
	 */
	public DataNode opIndex(size_t key) {
		return this.asArray()._values[key];
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
	size_t   current;
	
	
	/**
	 * Constructs a new reader from a slice and a starting offset
	 */
	this(ubyte[] data, size_t startOffset = 0) {
		this.data = data;
		this.current = startOffset;
	}
	
	
	/**
	 * Creates a new reader at a different offset: used by the pointer and cache types to go fetch data
	 * outside of their own location.
	 */
	Reader newReader(size_t offset) {
		return Reader(this.data, offset);
	}
	
	
	/**
	 * Reads $length bytes from the database
	 */
	ubyte[] read(size_t length) {
		size_t start = this.current;
		this.current += length;
		return this.data[start..this.current];
	}
	
	
	/**
	 * Reads $length bytes from the database and convert it to a native type of the appropriate endianness
	 */
	T read(T)(size_t length) {
		ubyte[T.sizeof] buffer = 0;
		buffer[$-cast(size_t)length..$] = this.read(length);
		return bigEndianToNative!T(buffer);
	}
	
	
	/**
	 * Reads $length byte into an integer type with the appropriate endianness and append extra most significant bits
	 */
	T read(T)(size_t length, T extrabits) if(isIntegral!T) {
		return this.read!T(length) + (extrabits << (length*8));
	}
	
	
	/**
	 * Reads a single byte from the database
	 */
	ubyte read() {
		return this.data[this.current++];
	}
}
