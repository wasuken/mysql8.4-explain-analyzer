#!/usr/bin/env python3
"""
現実的な偏りを持つサンプルデータ生成スクリプト
インデックス効果を最大化するための戦略的データ分布
"""

import mysql.connector
import random
import string
from datetime import datetime, timedelta
import sys
import uuid

# 現実的な偏りを持つデータ分布
FIRST_NAMES = ['Taro', 'Hanako', 'Yuki', 'Akiko', 'Hiroshi'] * 20 + ['John', 'Mary', 'David'] * 5  # 日本人が多い
LAST_NAMES = ['Tanaka', 'Suzuki', 'Sato', 'Takahashi'] * 25 + ['Smith', 'Johnson'] * 10
CITIES_JAPAN = ['Tokyo', 'Osaka', 'Yokohama', 'Nagoya', 'Kyoto', 'Fukuoka']
CITIES_OTHER = ['New York', 'Los Angeles', 'London', 'Berlin', 'Paris']

# 現実的な国分布（日本が圧倒的多数）
COUNTRIES_WEIGHTED = ['Japan'] * 70 + ['USA'] * 15 + ['Germany'] * 8 + ['UK'] * 4 + ['France'] * 3

# 商品カテゴリ（Electronics が人気）
CATEGORIES = ['Electronics'] * 40 + ['Clothing'] * 25 + ['Books'] * 15 + ['Home'] * 10 + ['Sports'] * 5 + ['Beauty'] * 3 + ['Toys'] * 2

# ステータス分布（delivered が圧倒的多数）
STATUSES_WEIGHTED = ['delivered'] * 60 + ['shipped'] * 25 + ['processing'] * 10 + ['pending'] * 3 + ['cancelled'] * 2

# 季節性を考慮した商品名
SEASONAL_PRODUCTS = {
    'winter': ['Heater', 'Coat', 'Boots', 'Scarf'],
    'spring': ['Jacket', 'Sneakers', 'Umbrella'],
    'summer': ['Fan', 'Shorts', 'Sunglasses', 'Swimwear'],
    'autumn': ['Sweater', 'Jacket', 'Boots']
}

PRODUCT_WORDS = ['Pro', 'Max', 'Ultra', 'Premium', 'Basic', 'Smart', 'Digital', 'Classic']
PAYMENT_METHODS = ['credit_card'] * 60 + ['debit_card'] * 20 + ['bank_transfer'] * 10 + ['paypal'] * 8 + ['cash'] * 2

DB_CONFIG = {
    'host': 'localhost',
    'port': 3366,
    'user': 'testuser',
    'password': 'testpass',
    'database': 'explain_test',
    'charset': 'utf8mb4'
}

def connect_db():
    """データベースに接続"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        print(f"💥 データベース接続エラー: {e}")
        sys.exit(1)

def get_season(date):
    """日付から季節を判定"""
    month = date.month
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    else:
        return 'autumn'

def generate_realistic_date():
    """現実的な日付分布（最近の注文が多い）"""
    if random.random() < 0.5:  # 50%は直近6ヶ月
        start = datetime.now() - timedelta(days=180)
        end = datetime.now()
    elif random.random() < 0.8:  # 30%は6-12ヶ月前
        start = datetime.now() - timedelta(days=365)
        end = datetime.now() - timedelta(days=180)
    else:  # 20%はそれ以前
        start = datetime.now() - timedelta(days=365*2)
        end = datetime.now() - timedelta(days=365)
    
    time_between = end - start
    days_between = time_between.days
    random_days = random.randrange(days_between)
    return start + timedelta(days=random_days)

def generate_realistic_registration_date():
    """顧客登録日（古い顧客が多い傾向）"""
    # 80%は1年以上前に登録
    if random.random() < 0.8:
        start = datetime.now() - timedelta(days=365*3)
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
    domain_weights = ['example.com'] * 40 + ['gmail.com'] * 30 + ['test.org'] * 20 + ['demo.net'] * 10
    domain = random.choice(domain_weights)
    return f"user_{unique_id}@{domain}"

def generate_realistic_price():
    """現実的な価格分布"""
    # 80%は100-1000円の商品、20%は高額商品
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
            
            # 国に応じて都市を選択
            if country == 'Japan':
                city = random.choice(CITIES_JAPAN)
            else:
                city = random.choice(CITIES_OTHER)
            
            registration_date = generate_realistic_registration_date().date()
            
            values_list.append("(%s, %s, %s, %s, %s, %s)")
            params.extend([email, first_name, last_name, registration_date, country, city])
        
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
        # 季節に応じた商品名
        season = random.choice(['winter', 'spring', 'summer', 'autumn'])
        seasonal_word = random.choice(SEASONAL_PRODUCTS[season])
        product_word = random.choice(PRODUCT_WORDS)
        product_name = f"{product_word} {seasonal_word}"
        
        category = random.choice(CATEGORIES)
        price = generate_realistic_price()
        
        # 人気商品は在庫多め、不人気商品は在庫少なめ
        if category == 'Electronics':
            stock_quantity = random.randint(50, 500)
        elif category in ['Clothing', 'Books']:
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
    
    batch_size = 50000  # バッチサイズを調整
    
    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        current_batch_size = batch_end - batch_start
        
        values_list = []
        params = []
        
        for i in range(current_batch_size):
            # 80/20の法則：20%の顧客が80%の注文
            if random.random() < 0.2:
                # アクティブ顧客（顧客IDの上位20%）
                customer_id = random.randint(int(min_customer_id + (max_customer_id - min_customer_id) * 0.8), max_customer_id)
            else:
                # 一般顧客
                customer_id = random.randint(min_customer_id, max_customer_id)
            
            # 人気商品に偏らせる（商品IDの上位30%が70%の注文）
            if random.random() < 0.7:
                product_id = random.randint(int(min_product_id + (max_product_id - min_product_id) * 0.7), max_product_id)
            else:
                product_id = random.randint(min_product_id, max_product_id)
            
            order_date = generate_realistic_date().date()
            
            # 現実的な注文数量（1-3個が大半）
            quantity_weights = [1] * 60 + [2] * 25 + [3] * 10 + [4, 5] * 2 + list(range(6, 11))
            quantity = random.choice(quantity_weights)
            
            unit_price = generate_realistic_price()
            total_amount = round(unit_price * quantity, 2)
            status = random.choice(STATUSES_WEIGHTED)
            
            # 配送国（顧客の国と異なる場合もある）
            if random.random() < 0.9:  # 90%は同じ国
                shipping_country = random.choice(COUNTRIES_WEIGHTED)
            else:  # 10%は異なる国（海外配送）
                shipping_country = random.choice(['Japan', 'USA', 'Germany', 'UK', 'France'])
            
            # 配送都市
            if shipping_country == 'Japan':
                shipping_city = random.choice(CITIES_JAPAN)
            else:
                shipping_city = random.choice(CITIES_OTHER)
            
            payment_method = random.choice(PAYMENT_METHODS)
            
            values_list.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            params.extend([customer_id, product_id, order_date, quantity, unit_price,
                          total_amount, status, shipping_country, shipping_city, payment_method])
        
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

def create_data_analysis_view(conn):
    """データ分析用のビューを作成"""
    cursor = conn.cursor()
    
    views = [
        """
        CREATE OR REPLACE VIEW v_order_analysis AS
        SELECT 
            o.order_id,
            o.order_date,
            o.total_amount,
            o.status,
            o.shipping_country,
            c.country as customer_country,
            c.registration_date,
            p.category,
            p.price as product_price,
            CASE 
                WHEN o.total_amount > 2000 THEN 'High'
                WHEN o.total_amount > 500 THEN 'Medium' 
                ELSE 'Low'
            END as order_value_tier,
            CASE
                WHEN DATEDIFF(CURDATE(), o.order_date) <= 90 THEN 'Recent'
                WHEN DATEDIFF(CURDATE(), o.order_date) <= 365 THEN 'This Year'
                ELSE 'Old'
            END as order_recency
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN products p ON o.product_id = p.product_id
        """,
        """
        CREATE OR REPLACE VIEW v_customer_summary AS
        SELECT 
            c.customer_id,
            c.country,
            c.registration_date,
            COUNT(o.order_id) as total_orders,
            SUM(o.total_amount) as total_spent,
            AVG(o.total_amount) as avg_order_value,
            MAX(o.order_date) as last_order_date,
            CASE 
                WHEN SUM(o.total_amount) > 10000 THEN 'VIP'
                WHEN SUM(o.total_amount) > 5000 THEN 'Premium'
                WHEN SUM(o.total_amount) > 1000 THEN 'Regular'
                ELSE 'New'
            END as customer_tier
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.country, c.registration_date
        """
    ]
    
    for view_sql in views:
        try:
            cursor.execute(view_sql)
            conn.commit()
        except Exception as e:
            print(f"ビュー作成エラー: {e}")
    
    cursor.close()
    print("📊 分析用ビューを作成完了")

def show_data_distribution(conn):
    """データ分布を表示"""
    cursor = conn.cursor()
    
    print("\n📈 データ分布レポート")
    print("=" * 50)
    
    # 国別分布
    cursor.execute("""
        SELECT shipping_country, COUNT(*) as orders, 
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders), 1) as percentage
        FROM orders 
        GROUP BY shipping_country 
        ORDER BY orders DESC
    """)
    
    print("\n🌍 国別注文分布:")
    for row in cursor.fetchall():
        print(f"  {row[0]:15} {row[1]:8,}件 ({row[2]:5.1f}%)")
    
    # ステータス別分布
    cursor.execute("""
        SELECT status, COUNT(*) as orders,
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders), 1) as percentage
        FROM orders 
        GROUP BY status 
        ORDER BY orders DESC
    """)
    
    print("\n📦 ステータス別分布:")
    for row in cursor.fetchall():
        print(f"  {row[0]:15} {row[1]:8,}件 ({row[2]:5.1f}%)")
    
    # 月別注文数
    cursor.execute("""
        SELECT DATE_FORMAT(order_date, '%Y-%m') as month, COUNT(*) as orders
        FROM orders 
        WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
        GROUP BY DATE_FORMAT(order_date, '%Y-%m')
        ORDER BY month DESC
        LIMIT 6
    """)
    
    print("\n📅 直近6ヶ月の注文推移:")
    for row in cursor.fetchall():
        print(f"  {row[0]} {row[1]:8,}件")
    
    cursor.close()

def main():
    """メイン処理"""
    print("🚀 現実的なサンプルデータ生成開始")
    print("💪 インデックス効果を最大化するデータ分布で攻める！")
    print("=" * 60)
    
    conn = connect_db()
    
    try:
        # MySQL設定を最適化
        optimize_mysql_for_bulk_insert(conn)
        
        # 現実的なデータ生成
        bulk_insert_realistic_customers(conn, 50000)
        bulk_insert_realistic_products(conn, 10000)
        bulk_insert_realistic_orders(conn, 1000000)
        
        # 分析用ビュー作成
        create_data_analysis_view(conn)
        
        # MySQL設定を元に戻す
        restore_mysql_settings(conn)
        
        # データ分布レポート
        show_data_distribution(conn)
        
        print("\n" + "🎉" * 20)
        print("💯 現実的なサンプルデータ生成完了！")
        print("👥 顧客: 50,000件 (日本70%、偏った分布)")
        print("📦 商品: 10,000件 (Electronics多め、季節性あり)")
        print("🛒 注文: 1,000,000件 (80/20法則、現実的偏り)")
        print("📊 総計: 1,060,000件の戦略的データ")
        
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
