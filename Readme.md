
A tool to extract key/value pairs from couchdb. This code is directly
derived from couchdb's file reading code, simplified and pruned down a
bit to remove things I'm not using. I've removed gen_servers and
tweaked the code to speed things up a bit.

# Licensing
This is licenced under the Apache License, Version 2.0, as was the
original couchdb 1.0.3 code this is derived from.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

# Notes on optimization:

Couchdb reads the last 4k of the file first, and generally tends to
move from the back to the front of the file. This can subvert the
readahead optimizations in the os; especially when you know you are
going to read the whole file. Prereading the file in advance can shave
a lot off of the walk of the btree, and the open code has been altered
to read the whole file in 1MB chunks before walking the btree.


