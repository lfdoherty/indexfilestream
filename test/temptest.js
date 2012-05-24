
var _ = require('underscorem')

var indexFile = require('./../indexfile')

var parsicle = require('parsicle')

var basicFormat = parsicle.make(function(parser){

	parser('entry', 'object', function(p){
		p.key('key').int();
		p.key('value').int();
	})
})


var w;

var state = {}
var currentSegment = {}
var readers = {
	entry: function(e, segmentIndex){
		var cur = currentSegment[e.key];
		if(cur !== undefined){
			w.replacedMapping(cur)
			//history[e.key].push(segmentIndex)
		}else{
			//history[e.key] = [segmentIndex]
		}
		currentSegment[e.key] = segmentIndex;
		state[e.key] = e.value;
		return 1;
	}
}
/*
var history = {}

function checkNot(seg){
	var fc = []
	_.each(currentSegment, function(si, key){
		//_.assertNot(seg === si)
		if(si === seg) fc.push(key)
	})
	if(fc.length > 0){
		console.log(JSON.stringify(fc))
		console.log('found still for ' + seg + ' ' + counts[seg] + ' ' + fc.length)
	}
	//console.log('passed')
}
var sched;
var counts = {}*/
var rewriters = {
	entry: function(e, oldSegmentIndex){
		if(currentSegment[e.key] === oldSegmentIndex){
			var cs = currentSegment[e.key] = w.writer.entry(e, 1)
			//history[e.key].push(cs)
		}
		/*if(sched !== oldSegmentIndex){
			counts[oldSegmentIndex] = 0
			var t = 0
			_.each(currentSegment, function(si, key){
				if(si === oldSegmentIndex) ++t
			})
			//console.log('t: ' + t + ' ' + oldSegmentIndex)
			process.nextTick(checkNot.bind(undefined, oldSegmentIndex))
			sched = oldSegmentIndex
		}
		++counts[oldSegmentIndex]*/
	}
}


var config = {
	path: 'tempdata/test', 
	readers: readers, 
	rewriters: rewriters, 
	format: basicFormat,
	maxSegmentLength: 10*1024
}
var w = indexFile.open(config, function(){
	
	//console.log('on read, got: ' + _.size(state) + ' keys')
	
	var api = {
		put: function(key, value){
			if(state[key] !== value){
				if(state[key] !== undefined){
					try{
						w.replacedMapping(currentSegment[key]);
					}catch(e){
						console.log('key: ' + key);
						console.log('history: ' + JSON.stringify(history[key]))
						throw e;
					}
				}
				// = w.getCurrentSegment()
				state[key] = value;
				//console.log('writing')
				var cs = currentSegment[key] = w.writer.entry({key: key, value: value}, 1)

				//if(history[key] === undefined) history[key] = [cs]
				//else history[key].push(cs)

				return cs;
			}
		},
		get: function(key){
			return state[key];
		}
	}
	
	setInterval(function(){test(api)}, 250);
})

function rand(n){
	return Math.floor(Math.random()*n);
}

function test(api){
	//console.log('testing')
	//var obj = {}
	for(var i=0;i<20*1000;++i){
		var k = rand(10000)
		var v = rand(100)
		//console.log(k + '->'+v)
		var c = api.put(k,v);
		//obj[k] = c
	}
	//console.log('obj: ' + JSON.stringify(obj))
	for(var i=0;i<50;++i){
		var key = rand(1000)
		_.assertEqual(api.get(key), state[key])
		//console.log('got ' + key + ' ' + api.get(key));	
	}
}
