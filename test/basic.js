
var _ = require('underscorem')

var indexFile = require('./../indexfile')

var parsicle = require('parsicle')

var basicFormat = parsicle.make(function(parser){

	parser('entry', 'object', function(p){
		p.key('key').int();
		p.key('value').int();
	})
})

exports['index-file-stream-tests'] = {
	environments: [
		[
			{
				name: 'basic'
			}
		]
	],
	load: ['basic,', function(done){
		
		var w;
		
		var state = {}
		
		var readers = {
			entry: function(e){
				state[e.key] = e.value;
			}
		}
		var rewriters = {
			entry: function(e){
				if(state[e.key] === e.value){//note that this rule is not quite perfect, if the value changed and then changed back we'll be duplicating an entry.
					w.writer.entry(e)
				}
			}
		}
		
		indexFile.open('test', readers, rewriters, parser, function(ifw){
			w = ifw;
			done.w = ifw;
			
			done.api = {
				put: function(key, value){
					if(state[key] !== value){
						w.writer.entry({key: key, value: value})
					}
				},
				get: function(key){
					return state[key];
				}
			}
			
			done()
		})
	}],
	/*
	load: ['basic,|ended,', function(done){
		var cur = 1;
		var many = 0;
		var manySegments = 0;
		function readCb(buf){
			for(var i=0;i<buf.length;++i){
				var v = buf[i];
				//console.log('read ' + v)
				_.assertEqual(v, cur);
				++cur;
				if(cur > 4) cur = 1;
				++many;
			}
		}
		function segmentCb(segmentIndex, wasDiscarded){
			_.assertEqual(cur, 1)
			//done.segments.push(manySegments)
			done.segments[segmentIndex] = true;
			if(wasDiscarded){
				console.log('discarded segmentIndex: ' + segmentIndex)
				_.assert(done.discarded[segmentIndex])
			}else{
				console.log('got segmentIndex: ' + segmentIndex)
				done.discarded[segmentIndex] = false
			}
			++manySegments;
		}
		done.segments = [];
		if(done.discarded === undefined) done.discarded = [];

		sf.open('test', readCb, segmentCb, function(w){
			if(done.manySegments !== undefined){
				_.assertEqual(done.manySegments, manySegments)
			}
			if(done.dataLength !== undefined){
				_.assertEqual(done.dataLength, many)
			}
			console.log('load set manySegments to ' + manySegments)
			done.manySegments = manySegments;
			done.dataLength = many;
			_.assertFunction(w.end)
			done.w = w;
			done()
		})
	}],
	write: ['ended>load', function(done){
		var testBuf = new Buffer(4);
		testBuf[0] = 1;
		testBuf[1] = 2;
		testBuf[2] = 3;
		testBuf[3] = 4;
		done.w.write(testBuf);
		done.dataLength += 4;
		console.log('wrote 4')
		done()
	}],
	segment: ['write', 'ended>load', function(done){
		done.w.segment();
		++done.manySegments
		console.log('increase manySegments to ' + done.manySegments)
		done()
	}],
	discard: ['write', 'discard>segment', 'ended>load', function(done){
		//done.w.segment();
		//++done.manySegments
		var candidates = [];
		//console.log('discarded len: ' + done.discarded.length)
		for(var i=0;i<done.manySegments-1;++i){
			var v = done.discarded[i];
			if(!v){
				candidates.push(i);
			}
		}
		_.assert(candidates.length > 0)
		var ci = Math.floor(Math.random()*candidates.length)
		var si = candidates[ci]
		done.discarded[si] = true;
		console.log('discarded: ' + si)
		done.w.discard(si);
		done.dataLength -= done.w.getSegmentSize(si)
		done()
	}],
	ended: ['ended>load', function(done){
		done.w.end();
		done.w = undefined;
		done();
	}]*/
}
