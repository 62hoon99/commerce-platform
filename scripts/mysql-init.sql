CREATE DATABASE IF NOT EXISTS order_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS inventory_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS product_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON order_db.* TO 'commerce'@'%';
GRANT ALL PRIVILEGES ON inventory_db.* TO 'commerce'@'%';
GRANT ALL PRIVILEGES ON product_db.* TO 'commerce'@'%';
FLUSH PRIVILEGES;
