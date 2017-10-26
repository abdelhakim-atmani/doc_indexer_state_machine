# Simple step function

A lambda function listen to a S3 bucket and start a step function which will:
   - exrtact some metadata from the file like email, file name
   - insert these metadata into a dynamoDB
   - index the content of the uploaded file into a cloudsearch service.

