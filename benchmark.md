# MySQL 8.4 EXPLAIN ANALYZE実践検証 - インデックスが効かないパターンを実データで検証してみた

MySQL学習、特にパフォーマンスチューニング学習の一環でExplain Analyzeを知った。

そもそもExplain自体がMySQLクエリがどのように実行するかについての情報を取得するものだ。

ただし、SQL自体の実行はしない。

これに対して、Analyzeは実行した上で各ステップごとの詳細情報、インデックス使ってるとかコストいくつだとか、実行時間を知ることができる。

この記事ではAIに生成してもらった検証コードを使って、主にインデックスの有無で検索がどのように変わるかをExplain Analyzeを使って見ていく。

ハンズオンで慣れようといった趣旨の記事だ。

## 検証環境

Docker + MySQL 8.4.3で以下の構成を構築：

```bash
git clone https://github.com/wasuken/mysql8.4-explain-analyzer
cd mysql8.4-explain-analyzer
make setup  # 自動セットアップ
```

**データ構成:**

- 顧客: 50,000件
- 商品: 10,000件  
- 注文: 1,000,000件
- 現実的な偏り（80/20法則、地域偏在など）を再現

## メインテーブル設計

```sql
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    order_date DATE NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled'),
    shipping_country VARCHAR(50) NOT NULL,
    shipping_city VARCHAR(100) NOT NULL,
    payment_method ENUM('credit_card', 'debit_card', 'bank_transfer', 'paypal', 'cash'),
    
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;
```

## ベンチマーク実行と結果

```bash
make benchmark
```

### 改善効果があったパターン

#### 大量日付範囲スキャン
```sql
SELECT order_id, order_date, total_amount, shipping_country
FROM orders
WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
ORDER BY order_date DESC
LIMIT 10000;
```

| 状態 | 実行時間 | actual time | 改善率 |
|------|----------|-------------|--------|
| インデックスなし | 0.576秒 | 1818.0ms | - |
| インデックスあり | 0.052秒 | 19.0ms | **95.7倍** |

**EXPLAIN ANALYZE結果（インデックスなし）:**
```
-> Limit: 10000 row(s)  (cost=34280 rows=10000) (actual time=6178..6223 rows=10000 loops=1)
    -> Sort: orders.order_date DESC, limit input to 10000 row(s) per chunk  (cost=34280 rows=995561) (actual time=6178..6193 rows=10000 loops=1)
```

**EXPLAIN ANALYZE結果（インデックスあり）:**
```
-> Limit: 10000 row(s)  (cost=112656 rows=10000) (actual time=0.204..78.3 rows=10000 loops=1)
    -> Filter: (orders.order_date >= <cache>((curdate() - interval 12 month)))  (cost=112656 rows=497780) (actual time=0.197..48.8 rows=10000 loops=1)
```

**詳細分析と比較:**

| 項目 | インデックスなし | インデックスあり | 分析 |
|------|-----------------|-----------------|------|
| **実行戦略** | Sort → Limit | Filter → Limit | インデックスにより戦略が根本的に変化 |
| **cost** | 34,280 | 112,656 | オプティマイザ予測はインデックスありで高コスト |
| **actual time (開始)** | 6,178ms | 0.204ms | **30,000倍の差** - インデックス効果が劇的 |
| **actual time (終了)** | 6,223ms | 78.3ms | 終了時点でも79倍の差 |
| **推定処理行数** | 995,561行 | 497,780行 | インデックスで処理対象が半減 |
| **処理方法** | 全件ソート後LIMIT | 条件フィルタ後LIMIT | フィルタファーストの効果 |

**考察:**

オプティマイザの`cost`予測（112,656 > 34,280）は間違っていたが、実際の`actual time`では圧倒的にインデックス版が高速。costは推定値であり、実測との乖離がある証拠。

インデックスが効いてるおかげで、Filterがかなり早くなってる。
Btreeはこういった範囲計算が強いね

#### 国別フィルタ検索
```sql
SELECT * FROM orders
WHERE shipping_country = 'Japan'
ORDER BY order_date DESC
LIMIT 8000;
```

| 状態 | 実行時間 | actual time | 改善率 |
|------|----------|-------------|--------|
| インデックスなし | 0.944秒 | 2098.0ms | - |
| インデックスあり | 0.093秒 | 57.5ms | **36.5倍** |

**EXPLAIN ANALYZE結果（インデックスなし）:**
```
-> Limit: 8000 row(s)  (cost=100654 rows=8000) (actual time=5773..5810 rows=8000 loops=1)
    -> Sort: orders.order_date DESC, limit input to 8000 row(s) per chunk  (cost=100654 rows=995561) (actual time=5773..5786 rows=8000 loops=1)
```

**EXPLAIN ANALYZE結果（インデックスあり）:**
```
-> Limit: 8000 row(s)  (cost=53070 rows=8000) (actual time=1.42..84.3 rows=8000 loops=1)
    -> Index lookup on orders using idx_country_date (shipping_country='Japan') (reverse)  (cost=53070 rows=497780) (actual time=1.41..57.5 rows=8000 loops=1)
```

**詳細分析と比較:**

| 項目 | インデックスなし | インデックスあり | 分析 |
|------|-----------------|-----------------|------|
| **実行戦略** | Sort → Limit | Index lookup → Limit | 複合インデックス`idx_country_date`が選択された |
| **cost** | 100,654 | 53,070 | コスト半減を正しく予測 |
| **actual time (開始)** | 5,773ms | 1.42ms | **4,000倍の差** |
| **actual time (終了)** | 5,810ms | 84.3ms | 69倍の改善 |
| **使用インデックス** | なし | `idx_country_date (reverse)` | 逆順ソートに対応 |
| **推定処理行数** | 995,561行 | 497,780行 | 効率的な絞り込み |

**考察:**

`shipping_country='Japan'`の条件で`idx_country_date`複合インデックスが選択され、さらに`(reverse)`でDESCソートも最適化。70%が日本のデータ分布でも十分な効果。

### インデックスが効かなかった（劣化した）パターン

#### 複雑範囲集計

```sql
SELECT 
    shipping_country,
    DATE_FORMAT(order_date, '%Y-%m') as month,
    COUNT(*) as order_count,
    AVG(total_amount) as avg_amount
FROM orders
WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)
  AND total_amount > 200
GROUP BY shipping_country, DATE_FORMAT(order_date, '%Y-%m')
ORDER BY order_count DESC;
```

| 状態 | 実行時間 | actual time | 結果 |
|------|----------|-------------|------|
| インデックスなし | 1.798秒 | 2996.0ms | - |
| インデックスあり | 1.920秒 | 2894.0ms | **劣化** |

**EXPLAIN ANALYZE結果（インデックスなし）:**
```
-> Limit: 100 row(s)  (actual time=11521..11521 rows=95 loops=1)
    -> Sort: order_count DESC, limit input to 100 row(s) per chunk  (actual time=11521..11521 rows=95 loops=1)
```

**EXPLAIN ANALYZE結果（インデックスあり）:**
```
-> Limit: 100 row(s)  (actual time=11450..11450 rows=95 loops=1)
    -> Sort: order_count DESC, limit input to 100 row(s) per chunk  (actual time=11450..11450 rows=95 loops=1)
```

**詳細分析と比較:**

| 項目 | インデックスなし | インデックスあり | 分析 |
|------|-----------------|-----------------|------|
| **実行戦略** | Sort → Limit | Sort → Limit | **戦略変化なし** |
| **actual time** | 11,521ms | 11,450ms | わずか71ms改善（誤差レベル） |
| **処理行数** | 95行 | 95行 | 同一 |
| **インデックス効果** | なし | なし | `GROUP BY` + `DATE_FORMAT`で無効化 |
| **コスト表示** | なし | なし | 集計処理でコスト計算複雑化 |

**考察:**

`GROUP BY`と`DATE_FORMAT`により、せっかく作った日付系インデックスが全く活用されない。集計処理では一時テーブル作成が支配的で、インデックスの恩恵が限定的。

explain analyzeの結果がほぼ同じ。

つまりそもそも設定したインデックスがほぼ機能してないように見える。

##### 調査

```sql
explain FORMAT = JSON
SELECT
    shipping_country,
    DATE_FORMAT(order_date, '%Y-%m') as month,
    COUNT(*) as order_count,
    AVG(total_amount) as avg_amount
FROM
    orders
WHERE
or  der_date >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)
AND total_amount > 200
GROUP BY
    shipping_country,
    DATE_FORMAT(order_date, '%Y-%m')
ORDER BY
    order_count DESC
;
```

explainしてみた。


```
    "select_id": 1,
    "cost_info": {
      "query_cost": "112656.46"
    },
    "ordering_operation": {
      "using_filesort": true,
      "grouping_operation": {
        "using_temporary_table": true,
        "using_filesort": false,
        "table": {
          "table_name": "orders",
          "access_type": "range",
          "possible_keys": [
            "idx_order_date",
            "idx_total_amount",
            "idx_date_amount",
            "idx_amount_date",
            "idx_country_date",
            "idx_covering_range"
          ],
          "key": "idx_covering_range",
          "used_key_parts": [
            "order_date"
          ],
          "key_length": "9",
          "rows_examined_per_scan": 497780,
          "rows_produced_per_join": 248889,
          "filtered": "50.00",
          "using_index": true,
          "cost_info": {
            "read_cost": "87767.48",
            "eval_cost": "24888.97",
            "prefix_cost": "112656.46",
            "data_read_per_join": "153M"
          },
          "used_columns": [
            "order_id",
            "order_date",
            "total_amount",
            "shipping_country"
          ],
          "attached_condition": "((`explain_test`.`orders`.`order_date` >= <cache>((curdate() - interval 18 month))) and (`explain_test`.`orders`.`total_amount` > 200.00))"
```

- そもそもwhere条件でほとんど絞れてない
- DATE_FORMAT関数を挟んでいるので全行処理が必要となり、集計計算が必要になることが原因の一つ。
- 集計後のデータをソートすることになるため、インデックスが利用できない

上記により、そもそも構造が悪いと言える。

##### 改善

小手先のテクニックでどうにかすることはできなかった。

条件を少し変えて調査したところ

whereをtotal_amount > 20000とかにしたら非常に高速になった。

大量返却系はどうしようもないくらいデータでてくるのでどうしようもないんだろうか。

全データの集計のため、集計テーブルなどを用意して対応すべきだろうか。

#### ダブル範囲検索

```sql
SELECT order_id, order_date, total_amount, shipping_country
FROM orders
WHERE order_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 MONTH) AND CURDATE()
  AND total_amount BETWEEN 200 AND 800
ORDER BY total_amount DESC, order_date DESC
LIMIT 3000;
```

| 状態 | 実行時間 | actual time | 結果 |
|------|----------|-------------|------|
| インデックスなし | 0.500秒 | 1828.0ms | - |
| インデックスあり | 2.978秒 | 2544.0ms | **大幅劣化** |

**EXPLAIN ANALYZE結果（インデックスなし）:**
```
-> Limit: 3000 row(s)  (cost=90822 rows=3000) (actual time=4075..4088 rows=3000 loops=1)
    -> Sort: orders.total_amount DESC, orders.order_date DESC, limit input to 3000 row(s) per chunk  (cost=90822 rows=995561) (actual time=4075..4080 rows=3000 loops=1)
```

**EXPLAIN ANALYZE結果（インデックスあり）:**
```
-> Limit: 3000 row(s)  (cost=530 rows=1500) (actual time=3401..3451 rows=3000 loops=1)
    -> Filter: ((orders.order_date between <cache>((curdate() - interval 6 month)) and <cache>(curdate())) and (orders.total_amount between 200 and 800))  (cost=530 rows=1500) (actual time=3401..3442 rows=3000 loops=1)
```

**詳細分析と比較:**

| 項目 | インデックスなし | インデックスあり | 分析 |
|------|-----------------|-----------------|------|
| **実行戦略** | Sort → Limit | Filter → Limit | 戦略変化したが結果は悪化 |
| **cost** | 90,822 | 530 | オプティマイザ予測では大幅改善 |
| **actual time (開始)** | 4,075ms | 3,401ms | 実際は674ms改善にとどまる |
| **actual time (終了)** | 4,088ms | 3,451ms | 637ms改善だが期待値より小さい |
| **推定処理行数** | 995,561行 | 1,500行 | 推定大幅減少だが実際は違う |
| **総実行時間** | 0.500秒 | 2.978秒 | **6倍劣化の謎** |

##### 調査

`cost=530`という超低コスト予測にも関わらず、実際は大幅劣化。`actual time`は改善しているが、Python側で測定した総実行時間は悪化。複合ソート`ORDER BY total_amount DESC, order_date DESC`がインデックス選択を混乱させている可能性。


よくわからんかったのでexplainしてみた。

```sql
explain format = JSON
SELECT
    order_id,
    order_date,
    total_amount,
    shipping_country
FROM
    orders
WHERE
    order_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
AND CURDATE()
AND total_amount BETWEEN 200 AND 800
ORDER BY
    total_amount DESC,
    order_date DESC
LIMIT 3000
;
```

```json
"key": "idx_amount_date",
"used_key_parts": ["total_amount", "order_date"],
"rows_examined_per_scan": 6000,
"rows_produced_per_join": 248889,
"filtered": "25.00",
"backward_index_scan": true,
"cost_info": {
  "read_cost": "87767.48",
  "eval_cost": "24888.97",
  "data_read_per_join": "153M"}
```

| 項目 | 値 | 問題 |
|------|-----|------|
| `rows_examined_per_scan` | 6,000 | スキャン効率は良い |
| `rows_produced_per_join` | 248,889 | **実際は25万行処理** |
| `filtered` | 25.00% | フィルタ効率が悪い |
| `data_read_per_join` | 153MB | **大量データ読み取り** |
| `backward_index_scan` | true | 逆順スキャンによる追加コスト |

オプティマイザは`idx_amount_date`を選択したが、実際は248,889行を処理し153MBのデータ読み取りが発生。`filtered=25%`により4分の3が無駄な処理となった。逆順インデックススキャンと大量データ読み取りがオーバーヘッドを生んでいる。

##### 改善

```sql
SELECT /*+ USE INDEX (orders, idx_date_amount) */
  order_id, order_date, total_amount, shipping_country
FROM orders
WHERE order_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 MONTH) AND CURDATE()
  AND total_amount BETWEEN 200 AND 800
ORDER BY order_date DESC, total_amount DESC
LIMIT 3000
```

USE INDEXを追加したところ、改善した。

[MySQL :: MySQL 8.0 リファレンスマニュアル :: 8.9.4 インデックスヒント](https://dev.mysql.com/doc/refman/8.0/ja/index-hints.html)

```
🔍 EXPLAIN詳細 (インデックスなし):

   -> Limit: 3000 row(s)  (cost=90822 rows=3000) (actual time=3984..3997 rows=3000 loops=1)

       -> Sort: orders.order_date DESC, orders.total_amount DESC, limit input to 3000 row(s) per chunk  (cost=90822 rows=995561) (actual time=3984..3988 rows=3000 loops=1)

```

```

🔍 EXPLAIN詳細 (インデックスあり):

   -> Limit: 3000 row(s)  (cost=112656 rows=3000) (actual time=10.9..58.9 rows=3000 loops=1)

       -> Filter: ((orders.order_date between <cache>((curdate() - interval 6 month)) and <cache>(curdate())) and (orders.total_amount between 200 and 800))  (cost=112656 rows=248890) (actual time=10.8..46.8 rows=3000 loops=1)

```

## Buffer Pool効率の確認

```sql
SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_read%';
```

```
+---------------------------------------+----------+
| Variable_name                         | Value    |
+---------------------------------------+----------+
| Innodb_buffer_pool_read_ahead_rnd     | 0        |
| Innodb_buffer_pool_read_ahead         | 0        |
| Innodb_buffer_pool_read_ahead_evicted | 0        |
| Innodb_buffer_pool_read_requests      | 21825943 |
| Innodb_buffer_pool_reads              | 9015     |
+---------------------------------------+----------+
```

**Hit Rate: 99.96%**

ほぼバッファプールに当たってるということになるが、

今回ベンチマークで繰り返し処理をいれているので今回は無関係だろうか。

## EXPLAIN ANALYZE出力例

### インデックス効果あり
```
-> Limit: 8000 row(s)  (cost=53070 rows=8000) (actual time=1.42..84.3 rows=8000 loops=1)
    -> Index lookup on orders using idx_country_date (shipping_country='Japan') (reverse)  (cost=53070 rows=497780) (actual time=1.41..57.5 rows=8000 loops=1)
```

### インデックス効果なし
```
-> Limit: 8000 row(s)  (cost=100654 rows=8000) (actual time=5773..5810 rows=8000 loops=1)
    -> Sort: orders.order_date DESC, limit input to 8000 row(s) per chunk  (cost=100654 rows=995561) (actual time=5773..5786 rows=8000 loops=1)
```

> Index lookup on orders using idx_country_date

Indexなんたらみたいなノリを見かけたら基本的にIndexは効いてると見れる。

データ量にもよるが、単純なソートやFilterについてはIndexきいてたら勝ち確定だと思う。

## 作成したインデックス一覧

```sql
-- 単一カラム
CREATE INDEX idx_shipping_country ON orders(shipping_country);
CREATE INDEX idx_order_date ON orders(order_date);
CREATE INDEX idx_total_amount ON orders(total_amount);
CREATE INDEX idx_status ON orders(status);

-- 複合インデックス
CREATE INDEX idx_date_amount ON orders(order_date, total_amount);
CREATE INDEX idx_amount_date ON orders(total_amount, order_date);
CREATE INDEX idx_country_date ON orders(shipping_country, order_date);
CREATE INDEX idx_status_amount ON orders(status, total_amount);

-- カバリングインデックス
CREATE INDEX idx_covering_range ON orders(order_date, total_amount, shipping_country, status);
```

ベースの思考として、Btreeなどのインデックスは少ない値を引っ張ってくる最適化となる。
なので当然だが全ソートなどの補助には一切ならない。

しかし、インデックスを貼ったカラムに対する条件やソートに関しては本当に高速に返却される。

今回は触れなかったが、更新が入るとインデックスも更新されるため、注意が必要だ。

また、複数のインデックスの判定は案外完璧ではなく、複数条件かつそれぞれ 異なるINDEXが絡まってるときに
スロークエリが出てきた場合はExplainなどでどう処理されてるかを見る価値はそれなりにある。
私もこれを知るまでは全く行ってこなかった。しようともしなかったが、見て、このINDEXが使ってねと書くだけで
それなりに早くなるのではっきりいってやらないのはコスパが悪い。

## 学んだこと・まとめ

- explain, explain analyze
- 簡単な使い方
- 簡単な見方
- 簡単な調査方法
