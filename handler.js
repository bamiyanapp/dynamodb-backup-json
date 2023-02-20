import { writeFileSync } from 'fs';
import { DynamoDB } from 'aws-sdk';

const { DYNAMODB_TABLE } = process.env;
const { SLEEP } = process.env;
const { BATCH_SIZE } = process.env;
const { KEY } = process.env;

const dynamo = new DynamoDB.DocumentClient();

let items = [];
function scan(lastEvaluatedKey) {
  const params = {
    TableName: DYNAMODB_TABLE,
    ExclusiveStartKey: lastEvaluatedKey || null,
  };
  console.log({ params });
  return dynamo.scan(params).promise();
}
function sleep(waitMsec) {
  var startMsec = new Date();
  // 指定ミリ秒間だけループさせる（CPUは常にビジー状態）
  while (new Date() - startMsec < waitMsec);
}
let k = 0;
function batchWrite(items) {
  const params = {
    RequestItems: {
      [DYNAMODB_TABLE]: items.map(item => ({
        PutRequest: {
          Item: { ...item },
        },
      })),
    },
  };
  sleep(SLEEP);
  return dynamo.batchWrite(params).promise();
}

async function getAll(lastEvaluatedKey) {
  const { Items, LastEvaluatedKey, Count } = await scan(lastEvaluatedKey);
  console.log({ LastEvaluatedKey, Count });
  items = [...items, ...Items];
  if (LastEvaluatedKey) {
    await getAll(LastEvaluatedKey);
  }
}

function writeToJSON(filename, data) {
//  console.log({ filename, count: data.length });
  writeFileSync(filename, JSON.stringify(data));
}

export async function exportToJSON() {
  await getAll();
  writeToJSON(
    `../../results/${DYNAMODB_TABLE}-${new Date().toISOString()}.json`,
    items,
  );
  return { statusCode: 200 };
}
export async function exportToJSON2() {
  const fs = require('fs');
  fs.unlink('./results/before.json',(error) => {});
  await getAll();
  writeToJSON(
    `../../results/before.json`,
    items,
  );
  return { statusCode: 200 };
}
function getKeyByValue(obj, key1,value1,key2,value2) {
//  return Object.keys(obj).find(key => obj[key] === value);
  for(let i = 0; i < obj.length; i++) {
//    const r = Object.values(obj[i]).includes(value);
    const r = Object.values(obj[i]).includes(value);

    if( r == true ) return true;
  }
  return false;
}
let i=0;
export async function importFromJSON() {
  const { default: items } = await import('./results/data.json');

  const r = exportToJSON2();
  const { default: items_b } = await import('./results/before.json');

  console.log('obj:' + items[0][KEY]);
//  for(let i = 0; i < items.length; i++) {
  let j=0;
  let items_import = [];
  for(let i = 0; i < 10; i++) {
      const r = getKeyByValue(items_b,items[i][KEY])
//      console.log("return:" + r);
    if ( r == true) {
      console.log("Found " + items[i][KEY] + " value");
    }else{
      console.log("Not found " + items[i][KEY] + " value");
      items_import[j] = items[i];
      j++;
    }
  }
  console.log('items_import:' + items_import.length);

  return 0;

  const startTime = Date.now(); // 開始時間
  const batchSize = BATCH_SIZE;
  const tableName = DYNAMODB_TABLE;

  const b      = items.length
  const batchItems = new Array(Math.ceil(items.length / batchSize)).fill()

  if (b >= batchSize) {
    for(let i = 0; i < Math.ceil(b / batchSize); i++) {
      const j = i * batchSize;
      let l = 0;
      l = (i + 1) * batchSize;
      const p = items.slice(j, l); // i*cnt 番目から i*cnt+cnt 番目まで取得
      console.log('i:' + i + ',j:' + j + ',l:' + l);
      const results =batchWrite(p);
    }
  }

  console.log('items:' + items.length);
  console.log("end");
  const endTime = Date.now(); // 終了時間
  console.log("--------------------------------");
  process.stdout.write('required time:')
  console.log(endTime - startTime,"s"); // 何ミリ秒かかったかを表示する
  return { statusCode: 200 };
}
