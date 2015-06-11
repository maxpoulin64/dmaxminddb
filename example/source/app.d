import maxmind.db;
import std.stdio;

/**
 * Program entry point: takes an IP address as the input, and returns a
 */
int main(string[] args) {
	if(args.length < 3) {
		stderr.writeln("Usage: dmaxminddb database-file ip-address");
		return -1;
	}
	
	auto db = new Database(args[1]);
	auto result = db.lookup(args[2]);
	
	if(result is null) {
		writeln("IP address not found in database.");
		return -1;
	}
	else {
		printNode(result); writeln();
		return 0;
	}
}


/** Helper function that writes the identation for the printNode function */
void writeIndent(int count) {
	while(count-- > 0) {
		write("    ");
	}
}


/**
 * Walks through the entire data tree and formats it as JSON
 */
void printNode(DataNode node, int indent = 0) {
	switch(node.type) {
		case DataNode.Type.String:
			write("\"" ~ node.get!string ~ "\"");
			break;
		
		case DataNode.Type.Double:
			write(node.get!double.to!string);
			break;
		
		case DataNode.Type.Binary:
			write("null");
			break;
		
		case DataNode.Type.Uint16:
			write(node.get!ushort.to!string);
			break;
		
		case DataNode.Type.Uint32:
			write(node.get!uint.to!string);
			break;
		
		case DataNode.Type.Map:
			writeln("{");
			int first = 0;
			
			foreach(key, subnode; cast(DataNode.Map) node) {
				if(first++) {
					writeln(",");
				}
				
				writeIndent(indent+1);
				write("\"" ~ key ~ "\": ");
				printNode(subnode, indent+1);
			}
			
			writeln();
			writeIndent(indent);
			write("}");
			break;
		
		case DataNode.Type.Int32:
			write(node.get!int.to!string);
			break;
		
		case DataNode.Type.Uint64:
			write(node.get!ulong.to!string);
			break;
		
		case DataNode.Type.Array:
			writeln("[");
			int first;
			
			foreach(key, subnode; cast(DataNode.Array) node) {
				if(first++) {
					writeln(",");
				}
				
				writeIndent(indent);
				printNode(subnode, indent+1);
				writeln(",");
			}
			
			writeln();
			writeIndent(indent);
			write("]");
			break;
		
		case DataNode.Type.Boolean:
			write(node.get!bool ? "true" : "false");
			break;
		
		case DataNode.Type.Float:
			write(node.get!float.to!string);
			break;
		
		default:
	}
}
