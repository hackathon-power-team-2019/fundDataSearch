console.log('Loading fundDataSearch function');

const AWS = require('aws-sdk')
const S3 = require('aws-sdk/clients/s3');

const client = new S3({
	region: 'us-east-1'
});

AWS.config.logger = console;

var s3 = new AWS.S3();
exports.handler = async (event) => {
	//var searchSql = "SELECT * FROM S3Object product WHERE product.assetClass = '" + event["searchTerm"] + "'";
	var searchSql = "SELECT s.* FROM S3Object s";
	
    console.log('funDataSearch lamba called!\n'+ searchSql);
    
    const params = {
	    Bucket: 'fund-service-bucket',
	    Key: 'funddata.json',
	    ExpressionType: 'SQL',
        Expression: searchSql,
        InputSerialization: { 
        	'JSON': {
	        	'Type': 'Document'
        	}
    	},
    	OutputSerialization: { 
        'JSON': {
          'RecordDelimiter': '\n'
        }
      }
    };
	
	
	s3.selectObjectContent(params, (err, data) => {
	if (err) {
		// Handle error
		console.log('ERROR!' + err.toString());
		return;
	}

	// data.Payload is a Readable Stream
	const eventStream = data.Payload;
	console.log('data payload::\n' + data.Payload.toString());
	
	// Read events as they are available
	eventStream.on('data', (event) => {
		console.log('event has data!');
		if (event.Records) {
			console.log('found records!');
			// event.Records.Payload is a buffer containing
			// a single record, partial records, or multiple records
			process.stdout.write(event.Records.Payload.toString());
		} else if (event.Stats) {
			console.log(`Processed ${event.Stats.Details.BytesProcessed} bytes`);
		} else if (event.End) {
			console.log('SelectObjectContent completed');
		}
	});

	// Handle errors encountered during the API call
	eventStream.on('error', (err) => {
		console.log('ERROR!' + err.toString());
		switch (err.name) {
			// Check against specific error codes that need custom handling
		}
	});

	eventStream.on('end', () => {
		console.log('end of stream!');
		// Finished receiving events from S3
	});
});
	
	
    // TODO implement
    const response = {
        statusCode: 200,
        body: JSON.stringify('Hello from Lambda!'),
    };
    return response;
};
