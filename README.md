# dmaxminddb - A reader library for MaxMind's GeoLite2 databases written in D

## Introduction
This is a library that reads MaxMind's [GeoLite2](http://dev.maxmind.com/geoip/geoip2/geolite2/) database files. It 
provides a simple to use API that allows to look up IPv4 and IPv6 adresses and return a parsed tree of objects somewhat
native to the database format.

Since the database format allows for constructions that are not possible in the D language (such as integers and maps
in the same array), it is not easy to convert records to pure D types. A decoder to JSON might be made in the future
to avoid the strange `DataNode`.

It can perform an average of 25,000 lookups on the GeoIP-Country database and 40,000 lookups on the GeoIP-Cities
database. Most of the time is spent parsing the database structure and creating objects. Performance could probably be
improved if needed by loading the entire database in memory, avoiding all the bitshifting maths used to compress the
database file.


## Installation
If using dub,

```
"dependencies": {
	"dmaxminddb": "~master"
}
```

A standalone Makefile is yet to be made, but there's only two source-code files so it shouldn't cause too much trouble.


## Example

```d
auto db = new Database("/usr/share/GeoIP/GeoLite2-City.mmdb");
auto result = db.lookup(args[1]);

if(result is null) {
	writeln("IP address not found in database.");
	return -1;
}
else {
	writeln("This IP is located in: " ~ result.country.names.en.get!string);
	return 0;
}
```


## License
```
dmaxminddb
Copyright (c) 2015 All rights reserved.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3.0 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library.
```

See [LICENSE](./LICENSE)
