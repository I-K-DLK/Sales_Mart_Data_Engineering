-- Подключимся к внешним источникам 
 

CREATE EXTERNAL TABLE chanel_ext(distr_chan varchar(100), txtsh text)
LOCATION('gpfdist://172.16.128.90:8080/chanel.csv')
FORMAT 'CSV'(HEADER DELIMETER ';' null '');

SELECT * FROM chanel_ext;

/*
distr_chan|txtsh  |
----------+-------+
         1|Опт    |
         2|Розница|
*/
 

CREATE EXTERNAL TABLE price_ext(
	material int, 
	region varchar(20),
	distr_chan varchar(100),
	price int)
LOCATION('gpfdist://172.16.128.90:8080/price.csv')
FORMAT 'CSV'(HEADER DELIMETER ';' null '');

 

SELECT * FROM price_ext LIMIT 10;

/*
material|region|distr_chan|price|
--------+------+----------+-----+
  128047|R001  |1         | 7085|
  177415|R001  |2         | 1956|
  219595|R001  |2         | 7601|
  248819|R001  |2         | 4395|
  294075|R001  |2         | 7066|
  333636|R001  |2         | 6219|
  373043|R001  |2         | 5682|
  413697|R001  |2         | 1269|
  450444|R001  |1         | 5875|
  467658|R001  |1         | 5895|
*/


 

CREATE EXTERNAL TABLE product_ext(material varchar(20), asgrp int, brand int, matcateg varchar(4), matdirec int, txt text)
LOCATION('gpfdist://172.16.128.90:8080/product.csv')
FORMAT 'CSV'(HEADER DELIMETER ';' null '');


SELECT * FROM product_ext LIMIT 10;

/*
material|asgrp    |brand|matcateg|matdirec|txt                                                                        |
--------+---------+-----+--------+--------+---------------------------------------------------------------------------+
 1132805|000001258|11805|H       |24      |xИнд.заказ 127130-2 Кружка Gain белый/синий                                |
 1159090|000001006|14809|О       |18      |Тетрадь общая А5,48л,кл,скреп,обл.офсет,бл.офсет-2 Весёлые кактусы С3614-34|
 1176833|000004590|13633|H       |22      |Трикотаж с лог Футболка 160г кор рук вас XXL 129730-4                      |
 1200262|000004859|28260|П       |35      |Бутылка On the go 650 мл белая арт.11670111                                |
 1203639|000001765|13633|S       |41      |Журнал учета медицинских отходов класса Б в клинике30 л, мяг обл           |
 1218448|000002934|13633|S       |67      |Кронштейн ТАНС.45.144.000(14.П2-0.2-0.35-Ф2-ц) OPORA ENGINEERING OE-06167  |
 1234373|000003111|13633|S       |71      |Мешок для бережной стирки белья 50смx60см ДС-87 Домашний Сундук            |
 1262396|000002231|15088|И       |46      |Площадка отделения Brother (торм.) в сборе ADF MFC-L2700DWR (LX9751001)    |
 1287262|000000291|13633|S       |08      |Шкаф напольный 400х540х850, ясень шимосветлый                              |
 1318720|000005148|29501|P       |18      |Модуль Звук                                                                |
 */

 

CREATE EXTERNAL TABLE region_ext(region varchar(20), txt text)
LOCATION('gpfdist://172.16.128.90:8080/region.csv')
FORMAT 'CSV'(HEADER DELIMETER ';' null '');

SELECT * FROM region_ext LIMIT 10;

/*
region|txt            |
------+---------------+
R001  |Москва         |
R002  |Санкт-Петербург|
R003  |Саратов        |
R004  |Казань         |
*/

 

CREATE READABLE EXTERNAL TABLE plan_ext (
	"date" date,
	region varchar(20),
	matdirec varchar(20),
	quantity int4,
	distr_chan varchar(100))

LOCATION ('pxf://gp.plan?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=usr&PASS=usr')
ON ALL
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
  

CREATE READABLE EXTERNAL TABLE sales_ext (
	"date" date,
	region varchar(20),
	material varchar(20),
	distr_chan varchar(100),
	quantity int4,
	check_nm varchar(100),
	check_pos varchar(100))

LOCATION ('pxf://gp.sales?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=usr&PASS=usr')
ON ALL
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')



-- Создадим новые таблицы Plan  и Sales со структурой аналогичных внешних таблиц из БД Postgres


-- Партиционирование таблиц фактов выбираем помесячное, исходя из периодичности построения отчетности в конце текущего месяца
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
-- Создадим новые таблицы - справочники

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
