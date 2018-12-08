// Imports the Google Cloud client library
const {PubSub} = require(`@google-cloud/pubsub`);
const {BigQuery} = require('@google-cloud/bigquery');

// Creates a client
const pubsub = new PubSub();

/**
 * TODO(developer): Uncomment the following lines to run the sample.
 */
const subscriptionName = 'projects/siemens-internet-of-things/subscriptions/my-subscription'
const timeout = 10;

const projectId = "siemens-internet-of-things";
const datasetId = "tmmiot_dataset";
const tableId = "tmmiot_table_v4";
// const schema = "Name:string, Age:integer, Weight:float, IsMagic:boolean";

// Creates a client
const bigquery = new BigQuery({
  projectId: projectId,
});

// References an existing subscription
const subscription = pubsub.subscription(subscriptionName);

// Create an event handler to handle messages
let messages = 0;
const messageHandler = function(message) {
  console.log(`Received message ${message.id}:`);
  console.log(`\tData: ${message.data}`);
  console.log(`\tAttributes: ${message.attributes}`);
  
  var jsonMessage = JSON.parse(message.data);
  
// Inserts data into a table
bigquery
	.dataset(datasetId)
	.table(tableId)
	.insert(jsonMessage)
	.then(() => {
		console.log(`Inserted ${jsonMessage.length} rows`);
	})
	.catch(err => {
		if (err && err.name === 'PartialFailureError') {
			if (err.errors && err.errors.length > 0) {
				console.log('Insert errors:');
				err.errors.forEach(err => console.error(err));
			}
		} else {
			console.error('ERROR:', err);
		}
	});

// "Ack" (acknowledge receipt of) the message
messages += 1;
message.ack();
};
// Create an event handler to handle errors
const errorHandler = function(error) {
  // Do something with the error
  console.error(`ERROR: ${error}`);
};

// Listen for new messages until timeout is hit
subscription.on(`message`, messageHandler);
subscription.on(`error`, errorHandler);

process.stdin.resume();

process.on('SIGINT', function () {
  subscription.removeListener('message', messageHandler);
  subscription.removeListener(`error`, errorHandler);
  console.log('Messages: ' + messages);
  console.log('Got SIGINT.  Press Control-D to exit.');
});
