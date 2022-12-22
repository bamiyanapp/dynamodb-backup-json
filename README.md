## DynamoDB backup JSON

### 概要

- DynamoDB のテーブルから Scan で取得したデータを JSON で書き出す
- 書き出した JSON のデータを指定したテーブルにインポートする

### 準備

- `.env` を修正する
    - DYNAMO_TABLE に取得/投入したいテーブル名を設定する
    - PROFILE に必要に応じて対象のテーブルにアクセス可能な AWS のプロファイルを設定する
        - `default` でよければそのままで

```
DYNAMO_TABLE=table_name
PROFILE=my-profile
```

- ライブラリのインストール

```sh
yarn
```

### エクスポート


```sh
yarn start
```

- 実行結果が `results` 配下に生成される

```
% tree results
results
├── table_name-2021-04-05T16:21:08.402Z.json
├── table_name-2021-04-05T16:16:47.024Z.json
├── table_name-2021-04-05T16:18:51.486Z.json
├── table_name-2021-04-05T16:27:46.992Z.json
└── table_name-2021-04-05T16:31:15.827Z.json
```

- JSONファイルを Prettier でフォーマットしているが不要な場合は以下のコマンドでエクスポートのみできる
    - データ量が多いとフォーマットも時間がかかるのでスキップした方が速い

```
yarn invoke:export
```

### インポート

- インポートしたいデータの入ったファイルを `results/data.json` に配置する
- 以下のコマンドでデータを投入できる

```
yarn invoke:import
```
