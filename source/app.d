import maxmind.db;
import std.stdio;

int main(string[] args) {
	if(args.length < 2) {
		stderr.writeln("This program requires an IP address as its only argument");
		return -1;
	}
	
	string ip = args[1];
	auto db = new Database("/usr/share/GeoIP/GeoLite2-City.mmdb");
	
	writeln("Looking up address: " ~ ip);
	return 0;
}
