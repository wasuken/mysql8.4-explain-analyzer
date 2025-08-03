#!/usr/bin/env python3
"""
EXPLAIN ANALYZE 実践検証スクリプト（改良版）
劇的な差が出るクエリパターンでインデックス設計を検証
"""

import mysql.connector
import time
import re
from datetime import datetime
import json
import pandas as pd

DB_CONFIG = {
    'host': 'localhost',
    'port': 3366,
    'user': 'testuser',
    'password': 'testpass',
    'database': 'explain_test',
    'charset': 'utf8mb4'
}

class ImprovedBenchmark:
    def __init__(self):
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self.results = []
        
    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def extract_execution_time(self, explain_output):
        """EXPLAIN ANALYZEの出力から実行時間を抽出"""
        if not explain_output:
            return None
            
        pattern = r'actual time=[\d.]+\.\.(\d+\.?\d*)'
        matches = re.findall(pattern, explain_output)
        
        if matches:
            return float(matches[-1])
        return None
    
    def extract_rows_examined(self, explain_output):
        """検査された行数を抽出"""
        if not explain_output:
            return None
            
        pattern = r'rows=(\d+)'
        matches = re.findall(pattern, explain_output)
        
        if matches:
            return int(matches[0])
        return None
    
    def run_explain_analyze(self, query, description=""):
        """EXPLAIN ANALYZEを実行"""
        cursor = self.conn.cursor()
        
        try:
            explain_query = f"EXPLAIN ANALYZE {query}"
            start_time = time.time()
            cursor.execute(explain_query)
            result = cursor.fetchall()
            end_time = time.time()
            
            explain_output = ""
            if result:
                explain_output = "\n".join([str(row[0]) for row in result])
            
            execution_time = self.extract_execution_time(explain_output)
            rows_examined = self.extract_rows_examined(explain_output)
            
            return {
                'description': description,
                'query': query.strip(),
                'execution_time_ms': execution_time,
                'rows_examined': rows_examined,
                'explain_output': explain_output,
                'total_time_sec': end_time - start_time,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'description': description,
                'query': query.strip(),
                'execution_time_ms': None,
                'rows_examined': None,
                'explain_output': f"ERROR: {str(e)}",
                'total_time_sec': None,
                'timestamp': datetime.now().isoformat()
            }
        finally:
            cursor.close()
    
    def create_index(self, index_name, table, columns, description=""):
        """インデックスを作成"""
        cursor = self.conn.cursor()
        try:
            query = f"CREATE INDEX {index_name} ON {table}({columns})"
            print(f"🔧 インデックス作成: {description}")
            print(f"   SQL: {query}")
            start_time = time.time()
            cursor.execute(query)
            self.conn.commit()
            end_time = time.time()
            print(f"   ✅ 作成完了 ({end_time - start_time:.2f}秒)")
        except Exception as e:
            print(f"   ❌ エラー: {e}")
        finally:
            cursor.close()
    
    def drop_all_indexes(self):
        """全インデックスを削除"""
        cursor = self.conn.cursor()
        indexes_to_drop = [
            ('orders', 'idx_date_country'),
            ('orders', 'idx_country_date'),
            ('orders', 'idx_status_date'),
            ('orders', 'idx_amount_date'),
            ('orders', 'idx_covering'),
            ('orders', 'idx_optimal'),
            ('customers', 'idx_customer_country'),
            ('customers', 'idx_customer_reg_country')
        ]
        
        for table, index_name in indexes_to_drop:
            try:
                cursor.execute(f"DROP INDEX {index_name} ON {table}")
                self.conn.commit()
            except:
                pass
        cursor.close()
        print("🧹 全インデックス削除完了")
    
    def get_heavy_queries(self):
        """重いクエリパターンを定義"""
        return [
            {
                'name': 'hell_join_aggregation',
                'query': """
                SELECT c.country, c.city, 
                       COUNT(*) as order_count,
                       SUM(o.total_amount) as total_revenue,
                       AVG(o.total_amount) as avg_order_value,
                       MAX(o.total_amount) as max_order
                FROM customers c 
                JOIN orders o ON c.customer_id = o.customer_id
                WHERE o.order_date BETWEEN '2023-06-01' AND '2023-06-30'
                  AND o.total_amount > 500
                  AND c.registration_date < '2023-01-01'
                GROUP BY c.country, c.city
                HAVING COUNT(*) > 5
                ORDER BY total_revenue DESC
                LIMIT 50
                """,
                'description': '💀 地獄の結合集計クエリ'
            },
            {
                'name': 'subquery_nightmare',
                'query': """
                SELECT DISTINCT c.email, c.country,
                       (SELECT COUNT(*) FROM orders o2 
                        WHERE o2.customer_id = c.customer_id 
                          AND o2.status = 'delivered') as delivered_count,
                       (SELECT MAX(total_amount) FROM orders o3 
                        WHERE o3.customer_id = c.customer_id) as max_amount
                FROM customers c
                WHERE EXISTS (
                    SELECT 1 FROM orders o4 
                    WHERE o4.customer_id = c.customer_id 
                      AND o4.order_date >= '2023-01-01'
                      AND o4.total_amount > 800
                )
                ORDER BY max_amount DESC
                LIMIT 100
                """,
                'description': '👻 サブクエリ地獄'
            },
            {
                'name': 'complex_date_range',
                'query': """
                SELECT DATE_FORMAT(o.order_date, '%Y-%m') as month,
                       o.shipping_country,
                       o.status,
                       COUNT(*) as order_count,
                       SUM(o.total_amount) as revenue,
                       COUNT(DISTINCT o.customer_id) as unique_customers
                FROM orders o
                WHERE o.order_date BETWEEN '2023-01-01' AND '2023-12-31'
                  AND o.total_amount BETWEEN 100 AND 2000
                  AND o.status IN ('delivered', 'shipped')
                GROUP BY DATE_FORMAT(o.order_date, '%Y-%m'), o.shipping_country, o.status
                ORDER BY month, revenue DESC
                """,
                'description': '📅 複雑な日付範囲集計'
            },
            {
                'name': 'ranking_with_window',
                'query': """
                SELECT c.country,
                       c.email,
                       o.total_amount,
                       o.order_date,
                       ROW_NUMBER() OVER (PARTITION BY c.country ORDER BY o.total_amount DESC) as rank_in_country
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                WHERE o.order_date >= '2023-01-01'
                  AND o.status = 'delivered'
                HAVING rank_in_country <= 10
                ORDER BY c.country, rank_in_country
                """,
                'description': '🏆 ウィンドウ関数ランキング'
            }
        ]
    
    def get_index_strategies(self):
        """インデックス戦略パターンを定義"""
        return [
            {
                'name': 'no_index',
                'description': '❌ インデックスなし',
                'setup': lambda: None,
                'cleanup': lambda: None
            },
            {
                'name': 'single_indexes',
                'description': '🔸 単一カラムインデックス',
                'setup': lambda: [
                    self.create_index('idx_order_date', 'orders', 'order_date', '注文日'),
                    self.create_index('idx_country', 'orders', 'shipping_country', '配送国'),
                    self.create_index('idx_status', 'orders', 'status', 'ステータス')
                ],
                'cleanup': lambda: self.drop_all_indexes()
            },
            {
                'name': 'bad_composite',
                'description': '💩 悪い複合インデックス（逆順）',
                'setup': lambda: self.create_index('idx_bad_order', 'orders', 'shipping_country, status, order_date', '悪い順序'),
                'cleanup': lambda: self.drop_all_indexes()
            },
            {
                'name': 'good_composite',
                'description': '✨ 良い複合インデックス',
                'setup': lambda: [
                    self.create_index('idx_optimal_1', 'orders', 'order_date, total_amount, status', '日付→金額→ステータス'),
                    self.create_index('idx_customer_reg', 'customers', 'registration_date, country', '登録日→国')
                ],
                'cleanup': lambda: self.drop_all_indexes()
            },
            {
                'name': 'covering_index',
                'description': '🚀 カバリングインデックス',
                'setup': lambda: [
                    self.create_index('idx_covering', 'orders', 'order_date, shipping_country, status, total_amount, customer_id', 'カバリング'),
                    self.create_index('idx_customer_all', 'customers', 'customer_id, country, city, email, registration_date', '顧客カバリング')
                ],
                'cleanup': lambda: self.drop_all_indexes()
            }
        ]
    
    def run_benchmark_suite(self):
        """改良版ベンチマーク実行"""
        print("🔥 EXPLAIN ANALYZE 実践検証開始")
        print("=" * 80)
        
        queries = self.get_heavy_queries()
        strategies = self.get_index_strategies()
        
        for strategy in strategies:
            print(f"\n{strategy['description']}")
            print("-" * 70)
            
            # インデックス設定
            if strategy['setup']:
                setup_result = strategy['setup']()
                if isinstance(setup_result, list):
                    pass  # 複数インデックス作成済み
            
            # 各クエリを実行
            for query in queries:
                print(f"\n{query['description']}:")
                result = self.run_explain_analyze(query['query'], 
                                                f"{strategy['name']}_{query['name']}")
                
                result['strategy_name'] = strategy['name']
                result['strategy_description'] = strategy['description']
                result['query_name'] = query['name']
                result['query_description'] = query['description']
                
                self.results.append(result)
                
                if result['execution_time_ms']:
                    print(f"  ⏱️  実行時間: {result['execution_time_ms']:.1f}ms")
                    print(f"  📊 検査行数: {result['rows_examined']:,}行" if result['rows_examined'] else "  📊 検査行数: N/A")
                    if result['total_time_sec']:
                        print(f"  🕐 総実行時間: {result['total_time_sec']:.2f}秒")
                else:
                    print(f"  ❌ エラー: 実行失敗")
            
            # インデックス削除
            if strategy['cleanup']:
                strategy['cleanup']()
    
    def generate_impact_report(self):
        """インパクトの強いレポートを生成"""
        if not self.results:
            print("❌ ベンチマーク結果がありません")
            return
        
        print("\n" + "🎯" * 30)
        print("💥 劇的改善効果レポート")
        print("🎯" * 30)
        
        df = pd.DataFrame(self.results)
        
        for query_name in df['query_name'].unique():
            query_results = df[df['query_name'] == query_name].copy()
            query_results = query_results.dropna(subset=['execution_time_ms'])
            query_results = query_results.sort_values('execution_time_ms')
            
            if len(query_results) == 0:
                continue
                
            print(f"\n🔥 {query_results.iloc[0]['query_description']}")
            print("-" * 60)
            
            baseline_time = None
            fastest_time = None
            
            # ベースライン取得
            baseline_row = query_results[query_results['strategy_name'] == 'no_index']
            if not baseline_row.empty:
                baseline_time = baseline_row.iloc[0]['execution_time_ms']
            
            fastest_time = query_results.iloc[0]['execution_time_ms']
            
            for _, row in query_results.iterrows():
                time_ms = row['execution_time_ms']
                improvement = ""
                emoji = ""
                
                if baseline_time and baseline_time > 0:
                    if row['strategy_name'] == 'no_index':
                        improvement = " (ベースライン)"
                        emoji = "😱"
                    else:
                        ratio = baseline_time / time_ms
                        if ratio > 10:
                            improvement = f" ({ratio:.0f}倍高速化!!!)"
                            emoji = "🚀"
                        elif ratio > 5:
                            improvement = f" ({ratio:.1f}倍高速化!!)"
                            emoji = "⚡"
                        elif ratio > 2:
                            improvement = f" ({ratio:.1f}倍高速化!)"
                            emoji = "✨"
                        elif ratio > 1.1:
                            improvement = f" ({ratio:.1f}倍高速化)"
                            emoji = "📈"
                        else:
                            improvement = f" ({time_ms/baseline_time:.1f}倍低速化)"
                            emoji = "💩"
                
                print(f"{emoji} {row['strategy_description']:40} {time_ms:8.1f}ms{improvement}")
        
        # JSONで詳細保存
        with open('impact_results.json', 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        
        print(f"\n📁 詳細結果をimpact_results.jsonに保存")
        print(f"📊 総計 {len(self.results)} 件のテストを実行")

def main():
    """メイン処理"""
    benchmark = ImprovedBenchmark()
    
    try:
        # 全インデックス削除してクリーンスタート
        benchmark.drop_all_indexes()
        
        # ベンチマーク実行
        benchmark.run_benchmark_suite()
        
        # インパクトレポート生成
        benchmark.generate_impact_report()
        
    except KeyboardInterrupt:
        print("\n⏹️  ベンチマーク中断されました")
    except Exception as e:
        print(f"💥 エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # クリーンアップ
        try:
            benchmark.drop_all_indexes()
        except:
            pass

if __name__ == "__main__":
    main()
