/*
Pelichan is a disk-backed channel pipe

Basic operation is to constantly pipe messages from Source to Sink channels.
Whenever Sink blocks, all incoming messages start being stored on disk in a LevelDB database.
When Sink cleanups both incoming and previously stored messages will be sent to Sink.

Simplified operation diagram:

	  user data +--------+ direct fwd  +------+ consumers
	  --------->| SrcFwd |------------>| Sink |---->
	  SRC       +--------+             +------+
	store on Sink full |  +--------+      ^
	                   |  | DskRdr |------/
	                   V  +--------+
	              +------+   ^ read n forward (RnF)
	              | Disk |---/
	              +------+

DiskBufferedChan (DBC) has three primary states:

1. Normal Operation - When it forwards incoming messages and try to store them on disk, if queue is full

2. Halt - (after calling halt method) when all reading/forwarding stopped, but DBC still can receive objects into
disk queue via Store() method.

3. Closed - when object is halted and LevelDB backend is closed, DBC now can be disposed

How to abort:

1. Close incoming Source pipe

After that all messages stored on disk will be pumped to Sink channel, and Sink will be closed afterwards
Call Close to close LevelDB backend and you're done.
If you want to save something in LevelDB, you can first wait for DiskReader to close with WaitHalt() and then
inject some messages via Store method, just before calling Close.
If you call halt during this messages in Sink will be sucked back on disk.

2. Call Halt()

This will stop both SourceForwarder and DiskReader, and all messages on disk be stored there. Messages
buffered in Sink will be sucked back to disk, LevelDB will be open till call of Close, so you can put in
some messages via Store before closing it.

3. Call Close()

You can just call Close to halt and close DBC ASAP
*/
package pelichan
