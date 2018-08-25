Pelichan
========

A persistent buffered channel.


**Current state is:** beta-testing

[GoDoc](https://godoc.org/github.com/redrampage/pelichan)

 `Pelichan` provides an easy-to-use persistent(disk-buffered) channel-like
 structure, with an emphasis on minimization of message loss and allowing graceful 
 application restart.

Features
--------
 * Disk storage used as last resort, when forwarding buffer is full
 * Graceful stopping and restarting without buffered messages loss (hopefully)
 * Statistics
 * Ordering of messages are totally not guaranteed
 * Backed by leveldb (and `github.com/beeker1121/goque`)

Use cases
---------

 * Log sending applications

   You can tunnel your log message flow through `Pelichan` to temporary store
   them on local drive in case of network outage or maintenance downtime of log 
   receiver.
   
 * Other...

How to Use 
----------
    type MyMsg struct {
        content string
    }
    
    ...
    
    // Channel that will produce MyMsg messages
    var rawMsgSource chan interface{} = getMyMsgSource()
    
    ...
    

    // Function to map stored messages from `interface{}` back to desired type
    var mapFn = func(item *goque.Item) (interface{}, error) {
        var msg MyMsg 
	    err := item.ToObject(&msg)
    	if err != nil {
	    	return nil, err
    	}
	    return obj, nil
    }

	dbc, bufMsgSource, err := pelichan.NewDiskBufferedChan(
		"./leveldb/",                   // Dir where leveldb with stored data will be
		100,                            // Size of the bufMsgSource channel
		mapFn,                          // Mapping function
		rawMsgSource,                   // Channel from where dbc will read messages
	)
	if err != nil {
		panic("Failed to initialize dbc")
	}
	defer dbc.Close()
	
	// Read buffered channel routine
	go func() {
	    for msg := range bufMsgSource {
	        fmt.Printf("Got message: %s\n", msg.content)
	    }
	}()
	
	// Other tasks are performed meanwhile...
	time.Sleep(30 * time.Second)
	
	// Note that pelichan will cease reading of source on halt, it's
	// user's concern to ensure that no messages buffered in this chan
	close(rawMsgSource)     // don't do that in real life, you should
	                        // signal `getMyMsgSource()` to stop&close instead
	dbc.Halt()
	
If your source/sink channels are not of `chan interface{}` type, you can use 
sample anon/deanon chan functions to workaround that.

    func anonChan(src chan MyMsg) (chan interface{}) {
	    ch := make(chan interface{})
    	go func() {
	    	defer close(ch)
		    for v := range src {
    			ch <- v
	    	}
    	}()
	    return ch
    }

    func deanonChan(src <-chan interface{}) (chan MyMsg) {
    	ch := make(chan MyMsg)
    	go func() {
    		defer close(ch)
	    	for v := range src {
   		    	ch <- v.(MyMsg)
	    	}
    	}()
	    return ch
    }
    
    ...
    
	dbc, bufMsgSource, err := pelichan.NewDiskBufferedChan(
		"./leveldb/",
		100,
		mapFn,
		anonChan(rawMsgSource),
	)
	
	var src chan MySrc = deanonChan(bufMsgSource)


Notes
-----
 * Be aware, that `Pelichan` is a potential bottleneck in your application,
   (although it tries not to) because of it disk-backed nature. Ensure that
   you underlying storage have enough performance to allow leveldb to store 
   whole planned incoming data stream.
 * Also note that after restart leveldb will be reading stored messages in 
   parallel with storing them, which will introduce even more disk load.
 * Pelichan doesn't protect *in-flight data* from loss in case of application
   crash because it uses disk only when in-memory buffer are full in order
   to maximize performance. If you need to store ALL messages on persistent
   storage you can use `github.com/beeker1121/goque` which provides a fully
   disk-backed FIFO structure.