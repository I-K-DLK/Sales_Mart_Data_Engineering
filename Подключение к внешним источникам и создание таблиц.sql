
-- Создадим новые таблицы Plan  и Sales со структурой аналогичных внешних таблиц из БД Postgres


-- Партиционирование таблиц фактов выбираем помесячное исходя из периодичности построения отчетности в конце текущего месяца
-- Распределение таблиц производим выбирая поле распределение, обеспечивающее наименьший перекос данных и по возможности учитывая
-- возможные поля для Joins (Проверяется в разделе загрузки данных в таблицы хранилища данных)

-- DROP TABLE shops.plan if exists;

CREATE TABLE shops.plan (
	"date" date,
	region varchar(20),
	matdirec varchar(20),
	quantity int4,
	distr_chan varchar(100)
	
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE("date")
(
PARTITION ym START ('2021-01-01'::date) END ('2021-08-01'::date) EVERY ('1 mon'::interval) 
WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)
);
 


-- DROP TABLE gp.sales if exists;

CREATE TABLE shops.sales (
	"date" date,
	region varchar(20),
	material varchar(20),
	distr_chan varchar(100),
	quantity int4,
	check_nm varchar(100),
	check_pos varchar(100)
	
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE("date")
(
PARTITION ym START ('2021-01-01'::date) END ('2021-08-01'::date) EVERY ('1 mon'::interval) 
WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)
);
-- Создадим новые таблицы 

-- 1) price

-- DROP TABLE if exists shops.price ;

CREATE TABLE shops.price (
	material varchar(20),
	region varchar(20),
	distr_chan varchar(100),
	price int
)
DISTRIBUTED REPLICATED;

-- 2) product

-- DROP TABLE if exists shops.product ;

CREATE TABLE shops.product (
	material varchar(20),
	asgrp int,
	brand int,
	matcateg varchar(4),
	matdirec varchar(20),
	txt text
)
DISTRIBUTED REPLICATED;

-- 3) chanel

-- DROP TABLE if exists shops.chanel ;

CREATE TABLE shops.chanel (
	distr_chan varchar(100),
	txtsh text
)
DISTRIBUTED REPLICATED;

-- 4) region

-- DROP TABLE if exists shops.region ;

CREATE TABLE shops.region (
	region varchar(20),
	txt text
)
DISTRIBUTED REPLICATED;
