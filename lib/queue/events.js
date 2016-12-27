/*!
 * kue - events
 * Copyright (c) 2013 Automattic <behradz@gmail.com>
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var redis = require('../redis');

/**
 * Job map.
 */

exports.jobs = {};

/**
 * Pub/sub key.
 */

exports.key = 'events';

/**
 * Add `job` to the jobs map, used
 * to grab the in-process object
 * so we can emit relative events.
 *
 * @param {Job} job
 * @api private
 */
exports.callbackQueue = [];

exports.add = function( job, callback ) {
  if( job.id ) {
    if(!exports.jobs[ job.id ])
      exports.jobs[ job.id ] = [];

    exports.jobs[ job.id ].push(job);
  }
//  if (!exports.subscribed) exports.subscribe();
  if( !exports.subscribeStarted[job.db] ) exports.subscribe(job.q);
  if( !exports.subscribed[job.db] ) {
    // if (Object.keys(exports.callbackQueue).length == 0) {
    //   exports.callbackQueue[job.db] = [];
    // }
    exports.callbackQueue.push(callback);
  } else {
    callback();
  }
};

/**
 * Remove `job` from the jobs map.
 *
 * @param {Job} job
 * @api private
 */

exports.remove = function( job ) {
  delete exports.jobs[ job.id ];
};

/**
 * Subscribe to "q:events".
 *
 * @api private
 */

exports.subscribe = function(q) {
//  if (exports.subscribed) return;
  if (typeof exports.subscribeStarted == 'undefined') {
    exports.subscribeStarted = {};
  }
  if (typeof exports.subscribed == 'undefined') {
    exports.subscribed = {};
  }
  if(typeof exports.subscribeStarted[q._options.redis.db] != 'undefined' && exports.subscribeStarted[q._options.redis.db] ) return;
  var client    = redis.pubsubClient();
  client.select(q._options.redis.db);
  client.on('message', exports.onMessage);
  client.subscribe(client.getKey(exports.key),  function() {
    exports.subscribed[q._options.redis.db] = true;
    while( exports.callbackQueue.length ) {
      process.nextTick(exports.callbackQueue.shift());
    }
  });
  exports.queue = require('../kue').singleton[q._options.name];
//  exports.subscribed = true;
  exports.subscribeStarted[q._options.redis.db] = true;
};

exports.unsubscribe = function() {
  var client               = redis.pubsubClient();
  client.unsubscribe();
  client.removeAllListeners();
  exports.subscribeStarted[q._options.db] = false;
};

/**
 * Message handler.
 *
 * @api private
 */

exports.onMessage = function( channel, msg ) {
  // TODO: only subscribe on {Queue,Job}#on()
  msg = JSON.parse(msg);

  // map to Job when in-process
  var jobs = exports.jobs[ msg.id ];
  if( jobs && jobs.length > 0 ) {
    for (var i = 0; i < jobs.length; i++) {
      var job = jobs[i];
      msg.args.push(job.q);
      if ([ 'complete', 'failed' ].indexOf(msg.event) > -1) {
        for (var j = 0; j < Object.keys(job._events).length; j++) {
          job._events[Object.keys(job._events)[j]].apply(job, msg.args);
        }
      }
      // job.emit.apply(job, msg.args);
      if( [ 'complete', 'failed' ].indexOf(msg.event) !== -1 ) exports.remove(job);
    }
  }
  // emit args on Queues
  msg.args[ 0 ] = 'job ' + msg.args[ 0 ];
  msg.args.splice(1, 0, msg.id);
  if( exports.queue ) {
    exports.queue.emit.apply(exports.queue, msg.args);
  }
};

/**
 * Emit `event` for for job `id` with variable args.
 *
 * @param {Number} id
 * @param {String} event
 * @param {Mixed} ...
 * @api private
 */

exports.emit = function( db, id, event ) {
  var client = redis.client()
    , msg    = JSON.stringify({
        id: id, event: event, args: [].slice.call(arguments, 1)
      });
  client.select(db);
  client.publish(client.getKey(exports.key), msg, function () {});
};
