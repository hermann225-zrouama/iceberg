-- select * from lakehouse.sales."sales_data_raw";

CREATE TABLE customers (
     id INT,
     first_name VARCHAR,
     last_name VARCHAR,
     age INT
   )
   PARTITION BY (truncate(1, last_name));


INSERT INTO customers (id, first_name, last_name, age) VALUES
     (1, 'John', 'Doe', 28),
     (2, 'Jane', 'Smith', 34),
     (3, 'Alice', 'Johnson', 22),
     (4, 'Bob', 'Williams', 45),
     (5, 'Charlie', 'Brown', 30);


INSERT INTO customers (id, first_name, last_name, age) VALUES
     (6, 'John', 'Doe', 28),
     (7, 'Jane', 'Smith', 34);


ALTER TABLE customers ADD COLUMNS (email VARCHAR);

ALTER TABLE customers ADD PARTITION FIELD truncate(1, first_name);

INSERT INTO customers (id, first_name, last_name, age, email) VALUES
  (7, 'Emily', 'Adams', 29, 'emily.adams@example.com'),
  (8, 'Frank', 'Baker', 35, 'frank.baker@example.com'),
  (9, 'Grace', 'Clark', 41, 'grace.clark@example.com');


select * from customers;


-------- 02 CREATION DE BRANCHE --------------
CREATE BRANCH development IN lakehouse;

USE BRANCH development IN lakehouse;

--- SANS OUBLIER DE MODIFIER LE CONTEXTE DE LA FENETRE

INSERT INTO customers (id, first_name, last_name, age, email) VALUES
  (9, 'Holly', 'Grant', 31, 'holly.grant@example.com'),
  (10, 'Ian', 'Young', 27, 'ian.young@example.com'),
  (11, 'Jack', 'Diaz', 39, 'jack.diaz@example.com');

SELECT * FROM customers AT BRANCH development;
SELECT count(*) FROM customers AT BRANCH main;

USE BRANCH main IN lakehouse;
MERGE BRANCH development INTO main IN lakehouse;

------ creation de TAG ----------------------------
CREATE TAG initial_load AT BRANCH main IN lakehouse;


INSERT INTO customers (id, first_name, last_name, age, email) VALUES
  (12, 'Kate', 'Morgan', 45, 'kate.morgan@example.com'),
  (13, 'Luke', 'Rogers', 33, 'luke.rogers@example.com');

USE TAG initial_load IN lakehouse;
SELECT * FROM customers;
USE BRANCH main IN lakehouse;

SELECT * FROM TABLE(table_files('customers'));
SELECT * FROM TABLE(table_history('customers'));
SELECT * FROM TABLE(table_manifests('customers'));
SELECT * FROM TABLE(table_partitions('customers'));

SELECT * FROM TABLE(table_snapshot('customers'));
SELECT * FROM customers;

SELECT * FROM customers AT SNAPSHOT '8626986426246576127';

--------------------
CREATE TABLE lakehouse.hidden_partition_test (
  id INT,
  category VARCHAR,
  created_at DATE
)
PARTITION BY (day(created_at));

INSERT INTO lakehouse.hidden_partition_test (id, category, created_at) VALUES
  (1, 'books', DATE '2023-01-01'),
  (2, 'books', DATE '2023-01-02'),
  (3, 'toys',  DATE '2023-02-15'),
  (4, 'games', DATE '2023-03-01'),
  (5, 'games', DATE '2023-03-02');


INSERT INTO lakehouse.hidden_partition_test (id, category, created_at) VALUES
  (6, 'books 2', CURRENT_TIMESTAMP()),
  (7, 'books 3', CURRENT_TIMESTAMP());

SELECT * from lakehouse.hidden_partition_test;

SELECT * FROM TABLE(table_partitions('lakehouse.hidden_partition_test'));


EXPLAIN PLAN FOR
SELECT * FROM lakehouse.hidden_partition_test
WHERE created_at > DATE '2023-03-01';
