-- sql/util/alltruncate.sql
-- 既存データの完全削除（外部キー制約対応版）

USE explain_test;

-- 外部キー制約チェックを一時的に無効化
SET FOREIGN_KEY_CHECKS = 0;

-- 全テーブルを TRUNCATE（高速削除）
TRUNCATE TABLE access_logs;
TRUNCATE TABLE orders;
TRUNCATE TABLE products;
TRUNCATE TABLE customers;

-- 外部キー制約チェックを再有効化
SET FOREIGN_KEY_CHECKS = 1;

-- AUTO_INCREMENT値をリセット
ALTER TABLE customers AUTO_INCREMENT = 1;
ALTER TABLE products AUTO_INCREMENT = 1;
ALTER TABLE orders AUTO_INCREMENT = 1;
ALTER TABLE access_logs AUTO_INCREMENT = 1;

-- 確認用：全テーブルの件数表示
SELECT 'customers' as table_name, COUNT(*) as count FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'access_logs', COUNT(*) FROM access_logs;
