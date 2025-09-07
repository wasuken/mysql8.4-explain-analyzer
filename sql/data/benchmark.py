#!/usr/bin/env python3
"""
EXPLAIN ANALYZE統合ベンチマーク - 接続同期問題完全修正版
sql/data/ 直下実行版、MySQLコネクション状態管理修正済み
"""

import mysql.connector
import time
import os

DB_CONFIG = {
    "host": "localhost",
    "port": 3366,
    "user": "testuser",
    "password": "testpass",
    "database": "explain_test",
    "charset": "utf8mb4",
}

# SQLクエリ定義（範囲系特化 + 新パターン）
QUERIES = {
    "date_range_massive": {
        "name": "💀 大量日付範囲スキャン",
        "sql": """
        SELECT order_id, order_date, total_amount, shipping_country
        FROM orders
        WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
        ORDER BY order_date DESC
        LIMIT 10000
        """,
    },
    "amount_range_heavy": {
        "name": "👻 金額範囲の重いスキャン",
        "sql": """
        SELECT *
        FROM orders
        WHERE total_amount BETWEEN 500 AND 1000
        ORDER BY total_amount DESC
        LIMIT 5000
        """,
    },
    "country_filter_massive": {
        "name": "📅 国別フィルタ大量検索",
        "sql": """
        SELECT *
        FROM orders
        WHERE shipping_country = 'Japan'
        ORDER BY order_date DESC
        LIMIT 8000
        """,
    },
    "double_range_nightmare_safe": {
        "name": "🔥 ダブル範囲検索地獄解消",
        "sql": """
        SELECT
          order_id, order_date, total_amount, shipping_country
        FROM orders
        WHERE order_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 MONTH) AND CURDATE()
          AND total_amount BETWEEN 200 AND 800
        ORDER BY order_date DESC, total_amount DESC
        LIMIT 3000
        """,
    },
    "status_range_combo": {
        "name": "⚡ ステータス + 範囲コンボ",
        "sql": """
        SELECT *
        FROM orders
        WHERE status IN ('delivered', 'shipped')
          AND total_amount > 300
        ORDER BY total_amount DESC
        LIMIT 6000
        """,
    },
    "complex_range_aggregation": {
        "name": "📊 複雑範囲集計",
        "sql": """
        SELECT 
            shipping_country,
            DATE_FORMAT(order_date, '%Y-%m') as month,
            COUNT(*) as order_count,
            AVG(total_amount) as avg_amount
        FROM orders
        WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)
          AND total_amount > 200
        GROUP BY shipping_country, DATE_FORMAT(order_date, '%Y-%m')
        ORDER BY order_count DESC
        LIMIT 100
        """,
    },
    "no_index_functions": {
        "name": "💩 関数でインデックス無効化",
        "sql": """
        SELECT *
        FROM orders
        WHERE YEAR(order_date) = 2024
          AND MONTH(order_date) >= 10
          AND total_amount * 1.1 > 500
        ORDER BY order_id DESC
        LIMIT 2000
        """,
    },
}


def clear_cursor_safely(cursor):
    """カーソル状態を安全にクリア"""
    try:
        # 残っている結果セットを全て消費
        while cursor.nextset():
            pass
        # 最後の結果も消費
        cursor.fetchall()
    except mysql.connector.Error:
        # 既に結果セットが空の場合
        pass
    except Exception:
        # その他のエラーは無視
        pass


def show_current_indexes(cursor):
    """現在のインデックス状況を表示"""
    print("🔍 現在のインデックス状況:")
    try:
        clear_cursor_safely(cursor)
        cursor.execute("""
            SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, NON_UNIQUE
            FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = 'explain_test'
              AND TABLE_NAME IN ('orders', 'customers')
              AND INDEX_NAME != 'PRIMARY'
            ORDER BY TABLE_NAME, INDEX_NAME
        """)
        indexes = cursor.fetchall()
        clear_cursor_safely(cursor)

        if indexes:
            for table, index_name, column, non_unique in indexes:
                print(f"   📋 {table}.{index_name} ({column})")
        else:
            print("   ✅ PRIMARY KEY以外のインデックスなし")
    except Exception as e:
        print(f"   ❌ インデックス確認エラー: {e}")
        clear_cursor_safely(cursor)


def drop_all_indexes(cursor):
    """インデックス強制削除"""
    print("🗑️ インデックス完全削除開始...")

    try:
        clear_cursor_safely(cursor)
        cursor.execute("""
            SELECT DISTINCT TABLE_NAME, INDEX_NAME
            FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = 'explain_test'
              AND TABLE_NAME IN ('orders', 'customers')
              AND INDEX_NAME != 'PRIMARY'
              AND INDEX_NAME NOT LIKE 'FK_%'
        """)
        existing_indexes = cursor.fetchall()
        clear_cursor_safely(cursor)

        for table_name, index_name in existing_indexes:
            try:
                drop_sql = f"DROP INDEX {index_name} ON {table_name}"
                cursor.execute(drop_sql)
                clear_cursor_safely(cursor)
                print(f"    🗑️ 削除成功: {table_name}.{index_name}")
            except Exception as e:
                clear_cursor_safely(cursor)
                print(f"    ❌ 削除失敗: {table_name}.{index_name} - {e}")

    except Exception as e:
        clear_cursor_safely(cursor)
        print(f"    💥 インデックス削除処理エラー: {e}")


def create_optimal_indexes(cursor):
    """範囲系に特化したインデックス"""
    print("⚡ 範囲系特化インデックス作成開始...")

    indexes = [
        ("orders", "idx_shipping_country", "shipping_country"),
        ("orders", "idx_order_date", "order_date"),
        ("orders", "idx_total_amount", "total_amount"),
        ("orders", "idx_status", "status"),
        # 複合インデックス（範囲 + ソート最適化）
        ("orders", "idx_date_amount", "order_date, total_amount"),
        ("orders", "idx_amount_date", "total_amount, order_date"),
        ("orders", "idx_country_date", "shipping_country, order_date"),
        ("orders", "idx_status_amount", "status, total_amount"),
        # カバリングインデックス（範囲検索用）
        (
            "orders",
            "idx_covering_range",
            "order_date, total_amount, shipping_country, status",
        ),
    ]

    for table, index_name, columns in indexes:
        try:
            clear_cursor_safely(cursor)
            create_sql = f"CREATE INDEX {index_name} ON {table}({columns})"
            cursor.execute(create_sql)
            clear_cursor_safely(cursor)
            print(f"    ✅ 作成成功: {table}.{index_name} ({columns})")
        except Exception as e:
            clear_cursor_safely(cursor)
            print(f"    ⚠️ 作成失敗: {table}.{index_name} - {e}")


def run_explain_analyze(cursor, sql):
    """EXPLAIN ANALYZEで実行して詳細情報取得"""
    try:
        clear_cursor_safely(cursor)
        explain_sql = f"EXPLAIN ANALYZE {sql}"
        cursor.execute(explain_sql)
        result = cursor.fetchall()
        clear_cursor_safely(cursor)

        explain_output = "\n".join([str(row[0]) for row in result])

        # actual timeとrowsを抽出
        import re

        actual_times = re.findall(r"actual time=[\d.]+\.\.([\d.]+)", explain_output)
        rows_examined = re.findall(r"rows=(\d+)", explain_output)

        actual_time = float(actual_times[-1]) if actual_times else 0
        total_rows = int(rows_examined[0]) if rows_examined else 0

        return {
            "actual_time_ms": actual_time,
            "rows_examined": total_rows,
            "explain_output": explain_output,
        }

    except Exception as e:
        clear_cursor_safely(cursor)
        return {
            "actual_time_ms": None,
            "rows_examined": None,
            "explain_output": f"ERROR: {str(e)}",
        }


def run_query_with_timer(cursor, sql):
    """通常実行 + EXPLAIN ANALYZE実行"""
    try:
        # 通常実行
        clear_cursor_safely(cursor)
        start = time.time()
        cursor.execute(sql)
        result = cursor.fetchall()
        clear_cursor_safely(cursor)
        end = time.time()
        execution_time = end - start

        # EXPLAIN ANALYZE実行
        explain_result = run_explain_analyze(cursor, sql)

        return {
            "execution_time": execution_time,
            "result_rows": len(result),
            "actual_time_ms": explain_result["actual_time_ms"],
            "rows_examined": explain_result["rows_examined"],
            "explain_output": explain_result["explain_output"],
        }

    except Exception as e:
        clear_cursor_safely(cursor)
        return {
            "execution_time": None,
            "result_rows": 0,
            "actual_time_ms": None,
            "rows_examined": None,
            "explain_output": f"ERROR: {str(e)}",
        }


def main():
    print("🔥 EXPLAIN ANALYZE統合ベンチマーク (sql/data/ 版)")
    print("=" * 60)

    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for query_key, query_info in QUERIES.items():
            name = query_info["name"]
            sql = query_info["sql"]

            print(f"\n{name}:")
            print("-" * 40)

            # インデックス削除前の状況確認
            show_current_indexes(cursor)

            # インデックスなしで実行
            drop_all_indexes(cursor)
            conn.commit()

            print("\n❌ インデックス削除後:")
            show_current_indexes(cursor)

            result1 = run_query_with_timer(cursor, sql)
            if not result1["execution_time"]:
                print(f"   ⚠️ クエリ実行失敗: {result1['explain_output']}")
                continue

            print(f"❌ インデックスなし:")
            print(f"   実行時間: {result1['execution_time']:.3f}秒")
            print(f"   結果行数: {result1['result_rows']}行")
            if result1["actual_time_ms"]:
                print(f"   actual time: {result1['actual_time_ms']:.1f}ms")
            if result1["rows_examined"]:
                print(f"   検査行数: {result1['rows_examined']:,}行")

            # インデックスありで実行
            create_optimal_indexes(cursor)
            conn.commit()

            print("\n✅ インデックス作成後:")
            show_current_indexes(cursor)

            result2 = run_query_with_timer(cursor, sql)
            if not result2["execution_time"]:
                print(f"   ⚠️ クエリ実行失敗: {result2['explain_output']}")
                continue

            print(f"✅ インデックスあり:")
            print(f"   実行時間: {result2['execution_time']:.3f}秒")
            print(f"   結果行数: {result2['result_rows']}行")
            if result2["actual_time_ms"]:
                print(f"   actual time: {result2['actual_time_ms']:.1f}ms")
            if result2["rows_examined"]:
                print(f"   検査行数: {result2['rows_examined']:,}行")

            # 改善効果計算
            if (
                result1["execution_time"]
                and result2["execution_time"]
                and result2["execution_time"] > 0
            ):
                time_improvement = result1["execution_time"] / result2["execution_time"]
                print(f"🚀 実行時間改善: {time_improvement:.1f}倍高速化")

            if (
                result1["actual_time_ms"]
                and result2["actual_time_ms"]
                and result2["actual_time_ms"] > 0
            ):
                actual_improvement = (
                    result1["actual_time_ms"] / result2["actual_time_ms"]
                )
                print(f"⚡ actual time改善: {actual_improvement:.1f}倍高速化")

            if result1["rows_examined"] and result2["rows_examined"]:
                rows_improvement = (
                    result1["rows_examined"] / result2["rows_examined"]
                    if result2["rows_examined"] > 0
                    else 1
                )
                print(f"📊 検査行数削減: {rows_improvement:.1f}倍減少")

            # EXPLAIN出力（簡略版）
            if result1["explain_output"] and "ERROR" not in result1["explain_output"]:
                print(f"\n🔍 EXPLAIN詳細 (インデックスなし):")
                lines = result1["explain_output"].split("\n")[:2]
                for line in lines:
                    print(f"   {line}")

            if result2["explain_output"] and "ERROR" not in result2["explain_output"]:
                print(f"\n🔍 EXPLAIN詳細 (インデックスあり):")
                lines = result2["explain_output"].split("\n")[:2]
                for line in lines:
                    print(f"   {line}")

    except mysql.connector.Error as e:
        print(f"💥 データベースエラー: {e}")
    except Exception as e:
        print(f"💥 予期しないエラー: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # 接続のクリーンアップ
        if cursor:
            try:
                clear_cursor_safely(cursor)
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

    print(f"\n🎉 EXPLAIN ANALYZEベンチマーク完了")


if __name__ == "__main__":
    main()
