
var sf = require('segmentedfile')

var _ = require('underscorem')

var EventEmitter = require('events').EventEmitter;
/*

Each object in the stream is one or more mappings.  The number of mappings must be returned from the read callback.
If a previous mapping is overridden during the read, indexfile must be notified, so that it can track the occupancy
of each segment in order to detect when it's time to fully duplicate and discard a segment.

Users must provide the 'rewriters' for use when the indexfile needs to move entries out of a segment.
The rewriters must rewrite only those entries or portions of entries which have not been rewritten already since
the given segment.

*/

exports.open = function(config, cb){

	_.assertLength(arguments, 2)

	var path = config.path
	var readers = config.readers
	var rewriters = config.rewriters
	var indexParser = config.format
	
	var minimumLoadRatio = config.minimumLoad || .5;
	var maxSegmentLength = config.maxSegmentLength || 1024*1024;

	var segmentCounts = [];
	var fullSegmentCounts = [];
	var segOff = -1;
	
	var discardOnSegment = false;

	var chained;

	var innerReaders = {}
	_.each(readers, function(readFunc, key){
		innerReaders[key] = function(e){
			var many = readFunc(e, segOff);
			_.assertInt(many)
			if(segmentCounts[segOff] !== undefined){
				segmentCounts[segOff] += many
				fullSegmentCounts[segOff] += many
			}else{
				_.errout('wtf')
			}
			//evaluateSegment(segOff)
		}
	})
	
	var reader;
	
	function readerWrapper(buf){
		reader(buf)
	}
	
	var discardedSegments = {}
	
	function segmentCb(si, wasDiscarded){
		_.assertBoolean(wasDiscarded)
		
		reader = indexParser.binary.stream.makeReader(innerReaders);
		++segOff;
		if(!wasDiscarded){
			segmentCounts[segOff] = 0
			fullSegmentCounts[segOff] = 0
		}else{
			discardedSegments[segOff] = true;
			//console.log('got segment cb: ' + segOff + ' ' + wasDiscarded)
		}
	}

	var handle;
	
	var sfw;
	var todoDiscard = {}
	function discardSegment(si){
		if(sfw === undefined || si === segOff){
			todoDiscard[si] = true;
			return;
		}
		
		//console.log('discarding segment ' + si)
		_.assertNot(discardedSegments[si])
		discardedSegments[si] = true;
		var innerRewriters = {}
		_.each(rewriters, function(rewriteFunc, key){
			innerRewriters[key] = function(e){
				var many = rewriteFunc(e, si);
			}
		})
		var tempReader = indexParser.binary.stream.makeReader(innerRewriters);
		sfw.readSegment(si, tempReader, function(){
			//console.log('finished reading ' + si)
			sfw.sync(function(){//ensure that we've written all rewritten data
				//console.log('synced ' + si)
				sfw.discard(si)
				if(chained){
					chained.discard(si)
				}
				segmentCounts[si] = undefined
				fullSegmentCounts[si] = undefined
			
				//console.log('finished reading segment being discarded ' + si)
			})
		})
	}
	function discardTodos(){
		Object.keys(todoDiscard).forEach(function(si){
			si = parseInt(si)
			discardSegment(si);
		})
		todoDiscard = {}
	}
	
	function flushWriter(){
		if(handle && handle.writer) handle.writer.flush();
	}
	var flushHandle = setInterval(flushWriter, 50)
	
	var chained;
	function makeNewSegment(){
		handle.writer.flush();
		sfw.segment();
		if(chained){
			chained.segment()
		}
		segmentCounts.push(0)
		fullSegmentCounts.push(0)
		handle.writer = makeWriter(sfw)
		
		discardTodos()
		++segOff;
	}
	
	var oldDrain;
	function makeWriter(w){
		_.assertDefined(w)
		var ew = new EventEmitter()
		ew.write = function(buf){
			//console.log('indexfile writing ' + buf.length)
			var res = w.write(buf);
			if(w.getCurrentSegmentSize() > maxSegmentLength){
				//console.log('oversize: ' + w.getCurrentSegmentSize())
				makeNewSegment();
				w = undefined;
			}
			return res;
		}
		if(oldDrain){
			w.removeListener('drain', oldDrain)
		}
		function newDrain(){
			ew.emit('drain');
		}
		w.on('drain', newDrain)

		oldDrain = newDrain;
		
		var pw = indexParser.binary.stream.makeWriter(maxSegmentLength, ew)
		
		var res = {}
		//_.each(pw, function(f, key){
		Object.keys(pw).forEach(function(key){
			var f = pw[key]
			
			if(key === 'flush'){
				res[key] = f;
				return;
			}
			res[key] = function(e, manyAdded){
				_.assertInt(manyAdded)
				var localSegOff = segOff;
				f(e);
				_.assertInt(segmentCounts[localSegOff])
				segmentCounts[localSegOff] += manyAdded;
				fullSegmentCounts[localSegOff] += manyAdded;
				return localSegOff;
			}
		})
		return res;
	}
	
	function evaluateSegment(si){
		var originalCount = fullSegmentCounts[si];
		var currentCount = segmentCounts[si]
		_.assertInt(originalCount)
		var load = currentCount/originalCount;

		//if(si === 285) console.log('load ' + load + ' ' + si)
		if(load < minimumLoadRatio){
			discardSegment(si);
		}
	}
	
	handle = {
		replacedMapping: function(originalSegment){
			//_.assertLength(arguments, 1)
			_.assertInt(originalSegment)
			
			if(segmentCounts[originalSegment] === undefined){
				_.errout('error, rewrite or some other failure by client - specified source segment has been discarded: ' + originalSegment);
			}
			
			if(segmentCounts[segOff] === undefined){
				if(discardedSegments[segOff]){
					_.errout('client error - segment already discarded ' + originalSegment)
				}else{
					_.errout('programmer error - segmentCounts undefined for ' + segOff)
				}
			}

			//_.assertInt(segmentCounts[originalSegment])
			//_.assertInt(segmentCounts[segOff])
			
			if(discardedSegments[originalSegment]) return;
	
			--segmentCounts[originalSegment]
			
			evaluateSegment(originalSegment)
		}
	}
	sf.open(path, readerWrapper, segmentCb, function(w){
		sfw = w
		
		//console.log('setting up indexfile writer')
		
		handle.getCurrentSegment = function(){
			return segOff;
		}
		
		handle.close = function(cb){
			//handle.writer.flush()
			_.assertFunction(cb)
			clearInterval(flushHandle)
			var cdl = _.latch(1+(config.chained?1:0), cb)
			flushWriter()
			sfw.end(cdl)
			chained.end(cdl)
		}

		function finish(){		
			handle.writer = makeWriter(w)
		
			discardTodos();
			cb()
		}

		if(config.chained){
			sf.open(config.chained, function(cw){
				chained = cw
				handle.chained = _.extend({}, cw);
				handle.chained.segment = function(){
					_.errout('cannot directly modify chained segmentation')
				}
				handle.chained.discard = function(){
					_.errout('cannot directly modify chained segmentation')
				}
				
				finish()
			})
		}else{
			finish()
		}
	})
	

	
	
	return handle;
}
