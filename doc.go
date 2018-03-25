package pelichan

// Pelichan is a disk-backed channel pipe
//
// Basic operation is to constantly pipe messages from `Source` to `Sink` channels.
// Whenever `Sink` blocks, all incoming messages start being stored on disk in a LevelDB database
// (via `github.com/beeker1121/goque` lib).
// When `Sink` clear-ups both incoming and previously stored messages will be sent to `Sink`.
//
// Simplified operation diagram:
//
// user data +--------+ direct fwd  +------+ consumers
// --------->| SrcFwd |------------>| Sink |---->
// SRC       +--------+             +------+
//        Sink full |  +--------+      ^
//                  |  | DskRdr |------/
//                  V  +--------+
//             +------+   ^ read n forward
//             | Disk |---/
//             +------+
//
// There is two ways to correctly terminate pipe's operation:
//
// - Close incoming `Source` pipe:
//   After that all messages stored on disk will be pumped to `Sink` channel, and `Sink will be closed afterwards`
//
// - Call Abort()
//   This will immediately cease piping operation and store all unsent messages on disk