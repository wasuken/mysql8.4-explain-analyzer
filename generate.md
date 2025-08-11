# 環境情報

[リポジトリはこちら](https://github.com/wasuken/mysql8.4-explain-analyzer)

# 🎯 この記事の目標

SQL パフォーマンス検証のための **テストデータ環境** を構築する。

---

# 📋 全体設計図

## 🏗️ アーキテクチャ概要

```
┌─────────────────────────────────────────────┐
│                Docker環境                    │
├─────────────────────────────────────────────┤
│  🐳 MySQL 8.4.3                           │
│  📊 phpMyAdmin (管理画面)                   │
│  ⚙️  カスタム設定 (my.cnf)                  │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│            Python データ生成スクリプト        │
├─────────────────────────────────────────────┤
│  🧹 clean_data_generator.py                │
│  📈 benchmark.py                           │
│  🔧 Makefile (自動化)                       │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│              現実的なテストデータ              │
├─────────────────────────────────────────────┤
│  👥 顧客: 50,000件                         │
│  📦 商品: 10,000件                         │
│  🛒 注文: 1,000,000件                      │
│  🎯 現実的な偏りを再現                      │
└─────────────────────────────────────────────┘
```

## 📊 データベース設計

### **customers テーブル**
```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    registration_date DATE NOT NULL,
    country VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL
);
```

### **products テーブル**
```sql
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INT NOT NULL DEFAULT 0
);
```

### **orders テーブル (メインターゲット)**
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
);
```

---

# 🚀 構築手順

## Step 1: Docker環境のセットアップ

### **compose.yml**
```yaml
services:
  mysql:
    build:
      context: .
      dockerfile: Dockerfile
    environment:
      MYSQL_ROOT_PASSWORD: rootpassword
      MYSQL_DATABASE: explain_test
      MYSQL_USER: testuser
      MYSQL_PASSWORD: testpass
    ports:
      - "3366:3306"
    volumes:
      - mysql_data:/var/lib/mysql
      - ./sql/init:/docker-entrypoint-initdb.d
    command: >
      --innodb_buffer_pool_size=1G
      --max_connections=200
      --slow_query_log=1
```

### **起動コマンド**
```bash
# 環境起動
docker compose up -d

# セットアップ実行
make setup
```

## Step 2: テストデータ生成

### **🎯 データの偏りを再現**

```python
# 80/20の法則：20%の顧客が80%の注文
if random.random() < 0.2:
    customer_id = random.randint(int(min_customer_id + (max_customer_id - min_customer_id) * 0.8), max_customer_id)
else:
    customer_id = random.randint(min_customer_id, max_customer_id)

# 日本が圧倒的多数 (70%)
COUNTRIES_WEIGHTED = ['Japan'] * 70 + ['USA'] * 15 + ['Germany'] * 8 + ['UK'] * 4 + ['France'] * 3

# 最近の注文が多い傾向
if random.random() < 0.5:  # 50%は直近6ヶ月
    start = datetime.now() - timedelta(days=180)
    end = datetime.now()
elif random.random() < 0.8:  # 30%は6-12ヶ月前
    start = datetime.now() - timedelta(days=365)
    end = datetime.now() - timedelta(days=180)
else:  # 20%はそれ以前
    start = datetime.now() - timedelta(days=365*2)
    end = datetime.now() - timedelta(days=365)
```

### **⚡ パフォーマンス最適化**

```python
def optimize_mysql_for_bulk_insert(conn):
    cursor.execute("SET autocommit = 0")
    cursor.execute("SET unique_checks = 0") 
    cursor.execute("SET foreign_key_checks = 0")

# 25,000件ずつバッチ処理
batch_size = 25000
for batch in range(0, total_count, batch_size):
    # バルクINSERTで高速化
    bulk_insert_batch(batch)
```

## Step 3: 自動化とベンチマーク準備

### **Makefile**
```makefile
setup:
	@echo "🔧 Docker環境起動"
	docker compose up -d
	@sleep 10
	@echo "📊 データ生成"
	sql/data/.venv/bin/python sql/data/clean_data_generator.py

benchmark:
	@echo "🚀 統合ベンチマーク実行"
	sql/data/.venv/bin/python sql/data/benchmark.py

clean:
	@echo "🧹 全データ削除"
	sql/data/.venv/bin/python sql/data/clean_data_generator.py
```

---

# 🎨 データ生成の工夫ポイント

## 1. **現実的な分布の再現**

| 要素 | 現実的な偏り |
|------|-------------|
| **顧客の国籍** | 日本70%, USA15%, その他15% |
| **注文ステータス** | delivered 60%, shipped 25%, その他15% |
| **商品カテゴリ** | Electronics 40%, Clothing 25%, その他35% |
| **注文時期** | 直近6ヶ月50%, 6-12ヶ月前30%, それ以前20% |

## 2. **季節性の考慮**
```python
SEASONAL_PRODUCTS = {
    'winter': ['Heater', 'Coat', 'Boots', 'Scarf'],
    'spring': ['Jacket', 'Sneakers', 'Umbrella'],
    'summer': ['Fan', 'Shorts', 'Sunglasses'],
    'autumn': ['Sweater', 'Jacket', 'Boots']
}
```

## 3. **ユニークなデータ生成**
```python
def generate_unique_email():
    unique_id = str(uuid.uuid4())[:8]
    return f"user_{unique_id}@example.com"
```

---

# 📊 生成されるデータの特徴

## **データ量**
- 👥 顧客: **50,000件**
- 📦 商品: **10,000件**  
- 🛒 注文: **1,000,000件**
- 📝 合計: **106万レコード**

## **クエリパフォーマンステスト対象**
```sql
-- 🔥 重い範囲検索
SELECT * FROM orders 
WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
ORDER BY order_date DESC 
LIMIT 10000;

-- 👻 複合条件検索
SELECT * FROM orders 
WHERE total_amount BETWEEN 500 AND 1000
  AND shipping_country = 'Japan'
ORDER BY total_amount DESC;

-- 📊 集計クエリ
SELECT shipping_country, COUNT(*), AVG(total_amount)
FROM orders 
WHERE order_date >= '2024-01-01'
GROUP BY shipping_country;
```

---

# 🔧 運用コマンド

## **環境セットアップ**
```bash
# 完全セットアップ
make setup

# データのみ再生成
make clean
```

## **接続確認**
```bash
# MySQL接続
mysql -h localhost -P 3366 -u testuser -p explain_test

# phpMyAdmin
http://localhost:8088
```

## **データ確認**
```sql
-- レコード数確認
SELECT 'customers' as table_name, COUNT(*) FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products  
UNION ALL
SELECT 'orders', COUNT(*) FROM orders;

-- インデックス状況
SHOW INDEX FROM orders;
```

---

# 🎯 次のステップ

この環境が完成したら、次は以下にチャレンジ

1. **📈 基本的なクエリ分析**
   - EXPLAIN の読み方
   - 実行計画の理解

2. **🔍 インデックス設計**  
   - 単一カラムインデックス
   - 複合インデックス
   - カバリングインデックス

3. **⚡ EXPLAIN ANALYZE**
   - 実際の実行時間測定
   - ボトルネック特定
   - 改善効果の定量化

4. **🏆 高度な最適化**
   - パーティショニング
   - クエリリライト
   - ハードウェア最適化

---

# 💡 まとめ

この環境で **現実的なデータでの SQL パフォーマンス検証** の基盤が整った。

**重要なポイント:**

- 🎯 **現実的な偏りを再現** → 実戦に近いテスト
- ⚡ **大量データでの検証** → スケーラビリティ確認  
- 🔧 **自動化による効率化** → 反復検証が容易
- 📊 **段階的な学習アプローチ** → 着実にスキルアップ

次は基本的な EXPLAIN から始めて、段階的に EXPLAIN ANALYZE まで進める。

が、その前にBufferPoolの確認の仕方の記事を作成するかも。
