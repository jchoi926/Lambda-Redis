const AWS = require('aws-sdk');
const Promise = require('bluebird');
const redis = require('redis');
const md5 = require('md5');

let env;
let config = {};
let redisClient;

exports.handler = handler;

function handler(event, context, callback) {
	console.log("EVENT", event);
	const payload = JSON.parse(event.Records[0].Sns.Message);
	const requestBody = JSON.parse(payload.event.body).value[0];
	const resourceData = requestBody.ResourceData;

	initialize()
		.then(() => {
			const subscriptionId = requestBody.SubscriptionId;
			const subscriptionIdHash = md5(subscriptionId);

			redisClient.hgetallAsync(`email_subscription:outlook:${subscriptionIdHash}`)
				.then(emailSubscription => {
					upsertMongo(emailSubscription, resourceData);
					callback(null, 'Lambda transfer: upsertMongo');
				})
				.catch(err => {
					console.log('Redis query error', err.message);
					callback(err);
				})
			;
		})
	;
};

/**
 * Bootstrap Lambda
 * @return {Promise}
 */
function initialize() {
	return getConfigParams()
		.then(() => setRedisClient())
	;
}

/**
 * Mongo lambda notifier
 * @param {string} userId
 * @param {Object} message
 */
function upsertMongo(userId, resourceData) {
	console.log('Lambda transfer: upsertMongo');
	const SNS = new AWS.SNS();
	const payload = {
		userId: userId,
		resourceData: resourceData,
		config: config
	};
	const params = {
		Message: JSON.stringify(payload),
		Subject: "Update mgDraft",
		TopicArn: "arn:aws:sns:us-west-1:931736494797:ci-draft-notify"
	};

	const upsertMongo = SNS.publish(params);
	upsertMongo.send();
}

/**
 * Set redis client
 * @return {Promise}
 */
function setRedisClient() {
	if (redisClient)
		return Promise.resolve(redisClient);

	return new Promise((resolve, reject) => {
		Promise.promisifyAll(redis.RedisClient.prototype);
		redisClient = redis.createClient({host: config.redis.host, port: parseInt(config.redis.port), password: config.redis.pass});

		redisClient.on('connect', () => {
			console.log('Redis client connected');
			resolve(redisClient);
		})
		.on('error', (err) => {
			console.log('Redis client error', err);
			reject(err);
		});
	});
}

/**
 * Get parameters from AWS Systems Manager Parameter Store
 * @return {Promise}
 */
function getConfigParams() {
	if (Object.keys(config).length > 0)
		return Promise.resolve(config);

	const S3 = Promise.promisifyAll(new AWS.S3({region: 'us-east-1'}));
	return S3.getObjectAsync({Bucket: 'ci-office-notification', Key: `${process.env.ENV}.json`})
		.then(s3Obj => {
			config = JSON.parse(s3Obj.Body.toString());
			return config;
		})
	;
}
