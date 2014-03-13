/**
 * Simple wrapper around aws-sqs, which is itself a simple wrapper
 * around `aws-lib`.
 */

var logger = require('winston')
var AwsSqs = require('aws-sqs');
var CONFIG;

/**
 * Set up a connection to SQS:
 */

exports.setup = function (config) {
	CONFIG = config;
	var aws = config.aws;
	sqs = new AwsSqs(aws.accessKeyId, aws.secretAccessKey, { region: aws.region });
};


/**
 * Keep a list of queue to name mappings:
 */

var mappings = {};

/**
 * Create a queue:
 */

exports.createQueue = function (queue, callback){

  /**
   * If we've already made a connection in this session then
   * reuse it:
   */

  if (!mappings[queue]){
    sqs.createQueue(queue, { VisibilityTimeout: CONFIG.sqs.visibilityTimeout },
        function(err, res){
      if (err) {
        logger.error('Error creating SQS queue: ', err);
      } else {
        mappings[queue] = res;
      }
      callback(err);
    });
  } else {
    callback(null);
  }
};

/**
 * Delete a queue:
 */

exports.deleteQueue = function (queue, callback){
  sqs.deleteQueue(mappings[queue], function(err, res){
    if (err) {
      logger.error('Error deleting SQS queue: ', err);
    } else {
      delete mappings[queue];
    }
    callback(err, res);
  });
};

/**
 * Receive a message:
 */

exports.receiveMessage = function (queue, callback){
  sqs.receiveMessage(mappings[queue], callback);
};

/**
 * Send a message:
 */

exports.sendMessage = function (queue, message, callback){
  sqs.sendMessage(mappings[queue], message, callback);
};

/**
 * Delete a message:
 */

exports.deleteMessage = function (queue, messageReceipt, callback){
  sqs.deleteMessage(mappings[queue], messageReceipt, callback);
};

/**
 * Get the number of messages in the queue:
 */

exports.countMessages = function (queue, callback){
  sqs.getQueueAttributes(
    mappings[queue]
  , ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
  , function (err, res){
    if (err){
      callback(err);
    } else {
      callback(null, {
        'count': Number(res.ApproximateNumberOfMessages)
      , 'in_progress': Number(res.ApproximateNumberOfMessagesNotVisible)
      });
    }
  });
};
