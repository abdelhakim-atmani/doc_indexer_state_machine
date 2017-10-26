var AWS = require('aws-sdk');
var doc = require('dynamodb-doc');
var db = new doc.DynamoDB();

var stepfunctions = new AWS.StepFunctions();

exports.start_state_machine = (event, context, callback) => {

    var params = {
      stateMachineArn: process.env.stateMachineArn,
      input: JSON.stringify(event)
    };
    stepfunctions.startExecution(params, function(err, data) {
      if (err) console.log(err, err.stack); // an error occurred
      else     console.log(data);           // successful response
    });
};

exports.saveNewCandidate = (event, context, callback) => {

    var tableName = process.env.metadataDBName;

    console.log(event);

    db.putItem({
        "TableName": tableName,
        "Item" : event
    }, function(err, data) {
        if (err) {
            callback(err);
        }
        else {
            callback(null, event);
        }
    });
};

exports.extract_email = (event, context, callback) => {
    var encodedData = Buffer.from(event.firstPageInBase64, 'base64').toString('utf8');
    var reg = new RegExp(/\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$/g);
    console.log(encodedData);
    var emails = encodedData.match(/([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+)/gi)
    if(emails) {
        event.email = emails[0];
    }
    delete event.firstPageInBase64;

    callback(null, event);
};

exports.extract_name = (event, context, callback) => {
    event.name = event.fileURI.split('/').pop().split('.')[0].replace('+', ' ');
    delete event.firstPageInBase64;
    callback(null, event);
};

exports.consolidate_data = (event, context, callback) => {
    var result = {};
    if(event instanceof Array) {
        event.forEach(item => {
            console.log(item);
            Object.keys(item).forEach(key => {
                result[key] = item[key];
            });
        });
    } else {
        result = event;
    }
    delete result.firstPageInBase64;
    callback(null, result);
};

var md5 = require('md5');
var readline = require('readline');
var s3 = new AWS.S3();
var documentClient = new AWS.DynamoDB.DocumentClient();

exports.generate_uid = (event, context, callback) => {
    // TODO implement
    var bucketName = event.Records[0].s3.bucket.name;
    var docKey = event.Records[0].s3.object.key.replace('+', ' ');
    var result = {};
    result.uid = md5(bucketName + '/' + docKey);
    result.fileURI = 's3://' + bucketName + '/' + docKey;
    result.bucketName = bucketName;
    result.docKey = docKey;

    var params = {Bucket: bucketName, Key: docKey}
    console.log(params);
    const rl = readline.createInterface({
        input: s3.getObject(params).createReadStream()
    });
    var first100Lines = '';
    var nbLines = 0;
    rl.on('line', function(line) {
        if(nbLines < 100) {
            first100Lines += line + '\n';
            nbLines++;
        }
    })
    .on('close', function() {
        result.firstPageInBase64 = new Buffer(first100Lines).toString('base64');
        var params = {
          TableName : process.env.metadataDBName,
          Key: {
            uid: result.uid
          }
        };
        documentClient.get(params, function(err, data) {
          if (err) callback(err);
          else {
              result.isNew = data.Item ? 0 : 1;
              callback(null, result);
          }
        });

    });

};

exports.index_doc = (event, context, callback) => {
    var params = {
        Bucket: event.bucketName,
        Key: event.docKey
    };
    var s3 = new AWS.S3();
    s3.getObject(params, function (err, data) {
        if (err) {
            console.log('file was not found : ERROR');
        }
        else {
            var contentText = data.Body.toString('utf8');
            addToIndex(event.uid, event.fileURI, contentText, context, process.env.docIndexerUri);
        }
    });
};

addToIndex = function (uid, document_uri, docContent, context, cloudSearchDomain) {

    var csd = new AWS.CloudSearchDomain({
        endpoint: cloudSearchDomain,
        apiVersion: '2013-01-01'
    });

    // see documentation at :
    var jbatch = [  {"type": "add",
                    "id": uid,
                    "fields": {"content": docContent,
 	                          "resourcename": document_uri }, } ];

    var params = { contentType: 'application/json', documents: JSON.stringify(jbatch) };

    csd.uploadDocuments(params, function(err, data) {
        if (err) {
            console.log('CloudSearchDomain ERROR');
            console.log(err);
            callback(err);
        }
        else {
            console.log('CloudSearchDomain SUCCESS');
        }
    });
};
