#!/usr/bin/env python3
"""
完全クリーンスタート版サンプルデータ生成スクリプト
既存インデックスを全削除してから現実的なデータを生成
"""

import mysql.connector
import random
import string
from datetime import datetime, timedelta
import sys
import uuid

# 現実的な偏りを持つデータ分布
FIRST_NAMES = ["Taro", "Hanako", "Yuki", "Akiko", "Hiroshi"] * 20 + [
    "John",
    "Mary",
    "David",
] * 5
LAST_NAMES = ["Tanaka", "Suzuki", "Sato", "Takahashi"] * 25 + ["Smith", "Johnson"] * 10
CITIES_JAPAN = ["Tokyo", "Osaka", "Yokohama", "Nagoya", "Kyoto", "Fukuoka"]
CITIES_OTHER = ["New York", "Los Angeles", "London", "Berlin", "Paris"]

# 現実的な国分布（日本が圧倒的多数）
COUNTRIES_WEIGHTED = (
    ["Japan"] * 70 + ["USA"] * 15 + ["Germany"] * 8 + ["UK"] * 4 + ["France"] * 3
)

# 商品カテゴリ（Electronics が人気）
CATEGORIES = (
    ["Electronics"] * 40
    + ["Clothing"] * 25
    + ["Books"] * 15
    + ["Home"] * 10
    + ["Sports"] * 5
    + ["Beauty"] * 3
    + ["Toys"] * 2
)

# ステータス分布（delivered が圧倒的多数）
STATUSES_WEIGHTED = (
    ["delivered"] * 60
    + ["shipped"] * 25
    + ["processing"] * 10
    + ["pending"] * 3
    + ["cancelled"] * 2
)

# 季節性を考慮した商品名
SEASONAL_PRODUCTS = {
    "winter": ["Heater", "Coat", "Boots", "Scarf"],
    "spring": ["Jacket", "Sneakers", "Umbrella"],
    "summer": ["Fan", "Shorts", "Sunglasses", "Swimwear"],
    "autumn": ["Sweater", "Jacket", "Boots"],
}

PRODUCT_WORDS = [
    "Pro",
    "Max",
    "Ultra",
    "Premium",
    "Basic",
    "Smart",
    "Digital",
    "Classic",
]
PAYMENT_METHODS = (
    ["credit_card"] * 60
    + ["debit_card"] * 20
    + ["bank_transfer"] * 10
    + ["paypal"] * 8
    + ["cash"] * 2
)

DB_CONFIG = {
    "host": "localhost",
    "port": 3366,
    "user": "testuser",
    "password": "testpass",
    "database": "explain_test",
    "charset": "utf8mb4",
}


def connect_db():
    """データベースに接続"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        print(f"💥 データベース接続エラー: {e}")
        sys.exit(1)


def drop_all_existing_indexes(conn):
    """既存の全インデックスを削除（PRIMARY KEY以外）"""
    print("🧹 既存インデックスの完全削除開始...")
    cursor = conn.cursor()

    # 全テーブルの全インデックス情報を取得
    tables = ["customers", "products", "orders", "access_logs"]

    for table in tables:
        try:
            # テーブルのインデックス一覧取得
            cursor.execute(f"SHOW INDEX FROM {table}")
            indexes = cursor.fetchall()

            dropped_indexes = []
            for index in indexes:
                key_name = index[2]  # Key_name
                # PRIMARY KEYと外部キーは削除しない
                if key_name != "PRIMARY" and not key_name.startswith("FK_"):
                    try:
                        cursor.execute(f"DROP INDEX {key_name} ON {table}")
                        dropped_indexes.append(key_name)
                    except mysql.connector.Error as e:
                        if "check that column/key exists" not in str(e):
                            print(f"  ⚠️  {table}.{key_name} 削除エラー: {e}")

            if dropped_indexes:
                print(f"  ✅ {table}: {', '.join(dropped_indexes)} を削除")
            else:
                print(f"  ✨ {table}: 削除対象インデックスなし")

        except mysql.connector.Error as e:
            print(f"  ⚠️  テーブル {table} の処理でエラー: {e}")

    conn.commit()
    cursor.close()
    print("🎯 既存インデックス削除完了！完全にクリーンな状態")


def truncate_all_tables(conn):
    """全テーブルのデータを削除"""
    print("💥 既存データの完全削除...")
    cursor = conn.cursor()

    try:
        # 外部キー制約を一時的に無効化
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        # 全テーブルを TRUNCATE
        tables = ["access_logs", "orders", "products", "customers"]
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table}")
            print(f"  🗑️  {table} テーブルをクリア")

        # AUTO_INCREMENT値をリセット
        cursor.execute("ALTER TABLE customers AUTO_INCREMENT = 1")
        cursor.execute("ALTER TABLE products AUTO_INCREMENT = 1")
        cursor.execute("ALTER TABLE orders AUTO_INCREMENT = 1")
        cursor.execute("ALTER TABLE access_logs AUTO_INCREMENT = 1")

        # 外部キー制約を再有効化
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()

        print("✅ 全データ削除完了")

    except mysql.connector.Error as e:
        print(f"💥 テーブル削除エラー: {e}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")  # 念のため復元

    cursor.close()


def generate_realistic_date():
    """現実的な日付分布（最近の注文が多い）"""
    if random.random() < 0.5:  # 50%は直近6ヶ月
        start = datetime.now() - timedelta(days=180)
        end = datetime.now()
    elif random.random() < 0.8:  # 30%は6-12ヶ月前
        start = datetime.now() - timedelta(days=365)
        end = datetime.now() - timedelta(days=180)
    else:  # 20%はそれ以前
        start = datetime.now() - timedelta(days=365 * 2)
        end = datetime.now() - timedelta(days=365)

    time_between = end - start
    days_between = time_between.days
    random_days = random.randrange(days_between)
    return start + timedelta(days=random_days)


def generate_realistic_registration_date():
    """顧客登録日（古い顧客が多い傾向）"""
    if random.random() < 0.8:
        start = datetime.now() - timedelta(days=365 * 3)
        end = datetime.now() - timedelta(days=365)
    else:
        start = datetime.now() - timedelta(days=365)
        end = datetime.now()

    time_between = end - start
    days_between = time_between.days
    random_days = random.randrange(days_between)
    return start + timedelta(days=random_days)


def generate_unique_email():
    """ユニークなメールアドレス生成"""
    unique_id = str(uuid.uuid4())[:8]
    domain_weights = (
        ["example.com"] * 40
        + ["gmail.com"] * 30
        + ["test.org"] * 20
        + ["demo.net"] * 10
    )
    domain = random.choice(domain_weights)
    return f"user_{unique_id}@{domain}"


def generate_realistic_price():
    """現実的な価格分布"""
    if random.random() < 0.8:
        return round(random.uniform(100, 1000), 2)
    else:
        return round(random.uniform(1000, 10000), 2)


def optimize_mysql_for_bulk_insert(conn):
    """MySQL設定を一時的にバルクインサート用に最適化"""
    cursor = conn.cursor()
    cursor.execute("SET autocommit = 0")
    cursor.execute("SET unique_checks = 0")
    cursor.execute("SET foreign_key_checks = 0")
    cursor.close()
    print("⚡ MySQL設定をバルクインサート用に最適化")


def restore_mysql_settings(conn):
    """MySQL設定を元に戻す"""
    cursor = conn.cursor()
    cursor.execute("SET unique_checks = 1")
    cursor.execute("SET foreign_key_checks = 1")
    cursor.execute("SET autocommit = 1")
    cursor.close()
    print("🔧 MySQL設定を復元")


def bulk_insert_realistic_customers(conn, count=50000):
    """現実的な分布の顧客データを生成"""
    print(f"👥 現実的な顧客データ {count:,} 件を生成中...")
    cursor = conn.cursor()

    batch_size = 25000

    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        current_batch_size = batch_end - batch_start

        values_list = []
        params = []

        for i in range(current_batch_size):
            email = generate_unique_email()
            first_name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)
            country = random.choice(COUNTRIES_WEIGHTED)

            if country == "Japan":
                city = random.choice(CITIES_JAPAN)
            else:
                city = random.choice(CITIES_OTHER)

            registration_date = generate_realistic_registration_date().date()

            values_list.append("(%s, %s, %s, %s, %s, %s)")
            params.extend(
                [email, first_name, last_name, registration_date, country, city]
            )

        values_clause = ",".join(values_list)
        query = f"""
        INSERT INTO customers (email, first_name, last_name, registration_date, country, city)
        VALUES {values_clause}
        """

        cursor.execute(query, params)
        conn.commit()
        print(f"  📊 {batch_end:,} / {count:,} 件完了")

    cursor.close()
    print("✅ 顧客データ生成完了")


def bulk_insert_realistic_products(conn, count=10000):
    """季節性を考慮した商品データを生成"""
    print(f"📦 季節性を考慮した商品データ {count:,} 件を生成中...")
    cursor = conn.cursor()

    values_list = []
    params = []

    for i in range(count):
        season = random.choice(["winter", "spring", "summer", "autumn"])
        seasonal_word = random.choice(SEASONAL_PRODUCTS[season])
        product_word = random.choice(PRODUCT_WORDS)
        product_name = f"{product_word} {seasonal_word}"

        category = random.choice(CATEGORIES)
        price = generate_realistic_price()

        if category == "Electronics":
            stock_quantity = random.randint(50, 500)
        elif category in ["Clothing", "Books"]:
            stock_quantity = random.randint(20, 200)
        else:
            stock_quantity = random.randint(5, 50)

        values_list.append("(%s, %s, %s, %s)")
        params.extend([product_name, category, price, stock_quantity])

    values_clause = ",".join(values_list)
    query = f"""
    INSERT INTO products (product_name, category, price, stock_quantity)
    VALUES {values_clause}
    """

    cursor.execute(query, params)
    conn.commit()
    cursor.close()
    print("✅ 商品データ生成完了")


def bulk_insert_realistic_orders(conn, count=1000000):
    """現実的な偏りを持つ注文データを生成"""
    print(f"🛒 注文データ {count:,} 件を生成中...")
    cursor = conn.cursor()

    # 顧客IDと商品IDの範囲を取得
    cursor.execute("SELECT MIN(customer_id), MAX(customer_id) FROM customers")
    min_customer_id, max_customer_id = cursor.fetchone()

    cursor.execute("SELECT MIN(product_id), MAX(product_id) FROM products")
    min_product_id, max_product_id = cursor.fetchone()

    batch_size = 50000

    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        current_batch_size = batch_end - batch_start

        values_list = []
        params = []

        for i in range(current_batch_size):
            # 80/20の法則：20%の顧客が80%の注文
            if random.random() < 0.2:
                customer_id = random.randint(
                    int(min_customer_id + (max_customer_id - min_customer_id) * 0.8),
                    max_customer_id,
                )
            else:
                customer_id = random.randint(min_customer_id, max_customer_id)

            # 人気商品に偏らせる（商品IDの上位30%が70%の注文）
            if random.random() < 0.7:
                product_id = random.randint(
                    int(min_product_id + (max_product_id - min_product_id) * 0.7),
                    max_product_id,
                )
            else:
                product_id = random.randint(min_product_id, max_product_id)

            order_date = generate_realistic_date().date()

            # 現実的な注文数量（1-3個が大半）
            quantity_weights = (
                [1] * 60 + [2] * 25 + [3] * 10 + [4, 5] * 2 + list(range(6, 11))
            )
            quantity = random.choice(quantity_weights)

            unit_price = generate_realistic_price()
            total_amount = round(unit_price * quantity, 2)
            status = random.choice(STATUSES_WEIGHTED)

            # 配送国（顧客の国と異なる場合もある）
            if random.random() < 0.9:
                shipping_country = random.choice(COUNTRIES_WEIGHTED)
            else:
                shipping_country = random.choice(
                    ["Japan", "USA", "Germany", "UK", "France"]
                )

            # 配送都市
            if shipping_country == "Japan":
                shipping_city = random.choice(CITIES_JAPAN)
            else:
                shipping_city = random.choice(CITIES_OTHER)

            payment_method = random.choice(PAYMENT_METHODS)

            values_list.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            params.extend(
                [
                    customer_id,
                    product_id,
                    order_date,
                    quantity,
                    unit_price,
                    total_amount,
                    status,
                    shipping_country,
                    shipping_city,
                    payment_method,
                ]
            )

        try:
            values_clause = ",".join(values_list)
            query = f"""
            INSERT INTO orders (customer_id, product_id, order_date, quantity, unit_price, 
                               total_amount, status, shipping_country, shipping_city, payment_method)
            VALUES {values_clause}
            """

            cursor.execute(query, params)
            conn.commit()
            print(f"  📊 {batch_end:,} / {count:,} 件完了")

        except mysql.connector.Error as e:
            print(f"  💥 エラー: {e}")
            break

    cursor.close()
    print("✅ 注文データ生成完了")


def show_final_status(conn):
    """最終状況を表示"""
    cursor = conn.cursor()

    print("\n" + "🎯" * 30)
    print("📊 最終データ状況レポート")
    print("🎯" * 30)

    # テーブル件数
    cursor.execute(
        "SELECT 'customers' as table_name, COUNT(*) as count FROM customers UNION ALL SELECT 'products', COUNT(*) FROM products UNION ALL SELECT 'orders', COUNT(*) FROM orders"
    )

    print("\n📈 テーブル別レコード数:")
    for row in cursor.fetchall():
        print(f"  📋 {row[0]:12} {row[1]:8,}件")

    # インデックス確認
    print(f"\n🔍 現在のインデックス状況:")
    tables = ["customers", "products", "orders"]

    for table in tables:
        cursor.execute(f"SHOW INDEX FROM {table}")
        indexes = cursor.fetchall()

        # PRIMARY KEY以外のインデックスをカウント
        custom_indexes = [idx[2] for idx in indexes if idx[2] != "PRIMARY"]

        if custom_indexes:
            print(f"  ⚠️  {table}: {', '.join(custom_indexes)} が残存")
        else:
            print(f"  ✅ {table}: インデックスなし（PRIMARY KEYのみ）")

    cursor.close()


def main():
    """メイン処理"""
    print("🚀 完全クリーンスタート版データ生成開始")
    print("💥 既存インデックス全削除 → 現実的データ生成")
    print("=" * 60)

    conn = connect_db()

    try:
        # ステップ1: 既存インデックスを完全削除
        drop_all_existing_indexes(conn)

        # ステップ2: 既存データを完全削除
        truncate_all_tables(conn)

        # ステップ3: MySQL設定を最適化
        optimize_mysql_for_bulk_insert(conn)

        # ステップ4: 現実的なデータ生成
        bulk_insert_realistic_customers(conn, 50000)
        bulk_insert_realistic_products(conn, 10000)
        bulk_insert_realistic_orders(conn, 1000000)

        # ステップ5: MySQL設定を元に戻す
        restore_mysql_settings(conn)

        # ステップ6: 最終状況レポート
        show_final_status(conn)

        print("\n" + "🎉" * 20)
        print("💯 完全クリーンスタート版データ生成完了！")
        print("🧹 既存インデックス: 完全削除済み")
        print("📊 現実的データ: 106万件生成済み")
        print("⚡ ベンチマーク準備: 完璧な状態")

    except Exception as e:
        print(f"💥 エラーが発生しました: {e}")
        conn.rollback()
        restore_mysql_settings(conn)
        import traceback

        traceback.print_exc()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
