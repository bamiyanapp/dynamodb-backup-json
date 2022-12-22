import { writeFileSync } from 'fs';
import { DynamoDB } from 'aws-sdk';

const { DYNAMODB_TABLE } = process.env;

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
  console.log({ params });
  sleep(1000);
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
  console.log({ filename, count: data.length });
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

export async function importFromJSON() {
  const { default: items } = await import('./results/data.json');

  const batchSize = 1;
  const batchItems = new Array(Math.ceil(items.length / batchSize))
    .fill()
    .map((_, i) => items.slice(i * batchSize, i * batchSize + batchSize));

  console.log(items.length);
  console.log(batchItems.length);
  const results = await Promise.all(batchItems.map(items => batchWrite(items)));
  console.dir({ results }, { depth: null });
  return { statusCode: 200 };
}
