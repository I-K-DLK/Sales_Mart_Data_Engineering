/*
1. Создайте базу данных std<номер пользователя> на 206 хосте.
 */


create database if not exists shops;

/*
2. Создайте в своей базе данных интеграционную таблицу ch_plan_fact_ext для доступа к данным витрины plan_fact_<YYYYMM> в системе Greenplum.
 */

DROP TABLE IF EXISTS shops.ch_plan_fact_ext;

CREATE TABLE IF NOT EXISTS shops.ch_plan_fact_ext
(
 	"Код региона" String,
	"Товарное направление" Int32,
	"Код канала сбыта" Int32,
	"Фактические количество" Int32,
	"Плановое количество" Int32,
	"Процент выполнения плана за месяц" Decimal(32,4),
	"Код товара" String
)
ENGINE = PostgreSQL('192.168.214.203:5432','adb','plan_fact_202103','usr','pass','shops');

SELECT * FROM shops.ch_plan_fact_ext;
 
 
/*
3. Создайте следующие словари для доступа к данным таблиц системы Greenplum:

ch_price_dict
ch_chanel_dict
ch_product_dict
ch_region_dict
 
 
 
 Сначала создадим таблицы, а затем на их базе создадим словари
 */


DROP TABLE shops.ch_price;
DROP TABLE shops.ch_chanel;
DROP TABLE shops.ch_product;
DROP TABLE shops.ch_region;


CREATE TABLE IF NOT EXISTS shops.ch_price(
   
	material String,
	region String,
	distr_chan String,
	price Int32
)
ENGINE = PostgreSQL('192.168.214.203:5432','adb','price','usr','pass','shops');



SELECT * FROM shops.ch_price;


CREATE TABLE IF NOT EXISTS shops.ch_product(
   
	material String,
	asgrp Int32,
	brand Int32,
	matcateg String,
	matdirec String,
	txt String
)
ENGINE = PostgreSQL('192.168.214.203:5432','adb','product','user','pass','shops');

 
SELECT * FROM shops.ch_product;

CREATE TABLE IF NOT EXISTS shops.ch_chanel(
   
	distr_chan String,
	txtsh String
)
ENGINE = PostgreSQL('192.168.214.203:5432','adb','chanel','user','pass','shops');


SELECT * FROM shops.ch_chanel; 


CREATE TABLE IF NOT EXISTS shops.ch_region(
   
	region String,
	txt String
)
ENGINE = PostgreSQL('192.168.214.203:5432','adb','region','user','pass','shops');

  
SELECT * FROM shops.ch_region; 

DROP DICTIONARY shops.ch_chanel_dict

CREATE DICTIONARY shops.ch_chanel_dict
(
distr_chan UInt64,
txtsh String
)
PRIMARY KEY distr_chan
SOURCE(POSTGRESQL(
    port 5432
    host '192.168.214.203'
    user 'shops'
    password 'pass'
    db 'adb'
    query 'SELECT distr_chan,  txtsh FROM shops.chanel'
))
LAYOUT(FLAT())
LIFETIME(300)

SELECT * FROM shops.ch_chanel_dict



DROP DICTIONARY shops.ch_region_dict

CREATE DICTIONARY shops.ch_region_dict
(
region String,
txt String
)
PRIMARY KEY region
SOURCE(POSTGRESQL(
    port 5432
    host '192.168.214.203'
    user 'shops'
    password 'pass'
    db 'adb'
    query 'SELECT region,  txt FROM shops.region'
))
LAYOUT(COMPLEX_KEY_HASHED)
LIFETIME(300)

SELECT * FROM shops.ch_region_dict
 
DROP DICTIONARY shops.ch_price_dict

CREATE DICTIONARY shops.ch_price_dict
(
material String,
region String,
distr_chan String,
price Int32
)
PRIMARY KEY material, region, distr_chan
SOURCE(POSTGRESQL(
    port 5432
    host '192.168.214.203'
    user 'shops'
    password 'pass'
    db 'adb'
    query 'SELECT material, region,  distr_chan, price FROM shops.price'
))
LAYOUT(COMPLEX_KEY_HASHED)
LIFETIME(300)


SELECT * FROM shops.ch_price_dict;
 
DROP DICTIONARY shops.ch_product_dict

CREATE DICTIONARY shops.ch_product_dict
(
	material String,
	asgrp Int32,
	brand Int32,
	matcateg String,
	matdirec String,
	txt String
)
PRIMARY KEY material
SOURCE(POSTGRESQL(
    port 5432
    host '192.168.214.203'
    user 'shops'
    password 'pass'
    db 'adb'
    query 'SELECT material, asgrp,  brand, matcateg, matdirec, txt  FROM shops.product'
))
LAYOUT(COMPLEX_KEY_HASHED)
LIFETIME(300)


SELECT * FROM shops.ch_product_dict;

/*
4. Создадим реплицированные таблицы ch_plan_fact на всех хостах кластера. Создадим распределённую таблицу ch_plan_fact_distr, 
выбрав для неё корректный ключ шардирования. Вставим в неё все записи из таблицы  ch_plan_fact_ext. 
 */

DROP TABLE   shops.ch_plan_fact;

CREATE TABLE  shops.ch_plan_fact ON CLUSTER default_cluster 
(
 
	"Код региона" String,
	"Товарное направление" Int32,
	"Код канала сбыта" Int32,
	"Фактические количество" Int32,
	"Плановое количество" Int32,
	"Процент выполнения плана за месяц" Decimal(32,4),
	"Код товара" String
)
ENGINE = ReplicatedMergeTree('/click/ch_mart_plan_fact/{shard}','{replica}')
ORDER BY ("Код региона", "Товарное направление", "Код канала сбыта")

 

INSERT INTO shops.ch_plan_fact SELECT * FROM shops.ch_plan_fact_ext;
 
-- DROP TABLE shops.ch_plan_fact_distr;


CREATE TABLE shops.ch_plan_fact_distr AS shops.ch_plan_fact
ENGINE = Distributed('default_cluster','shops','ch_plan_fact',"Код канала сбыта")
 
INSERT INTO shops.ch_plan_fact_distr SELECT * FROM shops.ch_plan_fact_ext;

 

 