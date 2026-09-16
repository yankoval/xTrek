// Execute the actual relay handler with SDK boundaries stubbed; no network/modules installed.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
(async () => {
  const [source, envelopePath, output] = process.argv.slice(2);
  const envelope = JSON.parse(fs.readFileSync(envelopePath)).sample_celery_envelope;
  const logs = [], presigned = [];
  let body;
  class Command { constructor(input) { this.input = input; } }
  class GetObjectCommand extends Command {}
  class PutObjectCommand extends Command {}
  class SQSClient { async send() { return {Messages:[{Body:JSON.stringify(body)}]}; } }
  const stubs = {
    '@aws-sdk/client-sqs': {SQSClient,ReceiveMessageCommand:Command},
    '@aws-sdk/client-s3': {S3Client:class {},GetObjectCommand,PutObjectCommand},
    '@aws-sdk/s3-request-presigner': {getSignedUrl:async(c,cmd)=>{presigned.push(cmd);return 'https://example.test/signed';}},
    '@smithy/node-http-handler': {NodeHttpHandler:class {}}
  };
  const sandbox={require:n=>{assert.ok(stubs[n]);return stubs[n]},module:{exports:{}},Buffer,
    process:{env:{API_KEY:'synthetic-api-key',YMQ_QUEUE_URL:'https://example.test/queue'}},
    console:{log:(...x)=>logs.push(x.join(' ')),warn:()=>{},error:()=>{}}};
  vm.runInNewContext(fs.readFileSync(source,'utf8'),sandbox,{filename:source});
  let passed=0;
  for(const ext of ['txt','json']) for(const format of ['yandex','flat','celery']) {
    const key='sign/1234567890_random_dataToSign.'+ext;
    if(format==='yandex')body={messages:[{details:{bucket_id:'test',object_id:key}}]};
    if(format==='flat')body={bucket_id:'test',object_id:key};
    if(format==='celery')body={...envelope,body:Buffer.from(JSON.stringify([[{bucket:'test',key}],{},{}])).toString('base64')};
    presigned.length=0;
    const response=await sandbox.module.exports.handler({httpMethod:'POST',headers:{'X-Api-Key':'synthetic-api-key'},body:JSON.stringify({action:'ReceiveMessage'})},{});
    assert.equal(response.statusCode,200);
    const links=JSON.parse(response.body).Messages[0].S3Links;
    assert.equal(links.originalKey,key);assert.equal(links.sigKey,key+'.sig');
    assert.equal(presigned[1].input.Bucket,'test');assert.equal(presigned[1].input.ContentType,'application/octet-stream');
    passed++;
  }
  const report={passed,findings:{request_api_key_is_logged:logs.some(l=>l.includes('synthetic-api-key'))}};
  fs.writeFileSync(output,JSON.stringify(report,null,2));console.log(JSON.stringify(report));
})().catch(e=>{console.error(e);process.exitCode=1});
