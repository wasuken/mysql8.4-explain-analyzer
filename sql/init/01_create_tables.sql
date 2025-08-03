-- ECサイトの注文テーブルを模擬したテスト用テーブル

USE explain_test;

-- 顧客テーブル
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    registration_date DATE NOT NULL,
    country VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 商品テーブル
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 注文テーブル（メインの検証対象）
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    order_date DATE NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') NOT NULL DEFAULT 'pending',
    shipping_country VARCHAR(50) NOT NULL,
    shipping_city VARCHAR(100) NOT NULL,
    payment_method ENUM('credit_card', 'debit_card', 'bank_transfer', 'paypal', 'cash') NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;

-- ログテーブル（アクセスログ風）
DROP TABLE IF EXISTS access_logs;
CREATE TABLE access_logs (
    log_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    ip_address VARCHAR(45) NOT NULL,
    user_agent TEXT,
    request_path VARCHAR(500) NOT NULL,
    request_method ENUM('GET', 'POST', 'PUT', 'DELETE') NOT NULL DEFAULT 'GET',
    response_code INT NOT NULL,
    response_time_ms INT NOT NULL,
    access_date DATE NOT NULL,
    access_datetime TIMESTAMP NOT NULL,
    
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;
