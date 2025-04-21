/*
1. Создадим 2 пользовательские функции для загрузки данных в созданные в GP таблицы: 
Загрузка данных в целевые таблицы будет производиться из внешних EXTERNAL таблиц.
Первая функция для загрузки справочников, вторая - для загрузки таблиц фактов.
Для таблиц справочников реализуем FULL загрузку (полная очистка целевой таблицы и полная вставка всех записей).
Для таблиц фактов реализуем загрузку методом DELTA_PARTITION - полная подмена партиций.
А также можно выполнить загрузку методом DELTA UPSERT - предварительное удаление по ключу и последующая вставка
записей из временной таблицы в целевую
 
*/

-- Создадим таблицу для логирования процесса загрузки

create table shops.logs(
log_id int NOT NULL,
log_timestamp timestamp NOT NULL DEFAULT NOW(),
log_type text NOT NULL,
log_msg text NOT NULL,
log_location text NOT NULL,
is_error bool NULL,
log_user text NULL DEFAULT "current_user"(),
constraint pk_log_id PRIMARY KEY (log_id)
)
DISTRIBUTED BY (log_id);

-- Создадим последовательность для заполнения таблицы с логамии

CREATE SEQUENCE shops.log_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 999999999999999999
	START 1;



-- Создадим функцию для логирования

CREATE OR REPLACE FUNCTION shops.f_write_log(p_log_type text, p_log_message text, p_location text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
	
	 
DECLARE
v_log_type text;
v_log_message text;
v_location text;
v_sql text;
v_res text;
 
 
BEGIN

v_log_type = upper(p_log_type);
v_location = lower(p_location);


IF v_log_type NOT IN ('ERROR','INFO') THEN
	RAISE EXCEPtION 'Illegal log type! USE one of: ERROR,INFO';
END IF;


RAISE NOTICE '%: %: <>% Location[%]', clock_timestamp(), v_log_type, p_log_message, v_location;

v_log_message := replace(p_log_message, '''', '''''');

v_sql := 'INSERT INTO shops.logs(log_id,log_type,log_msg, log_location, is_error, log_timestamp, log_user)
			VALUES (' ||nextval('shops.log_id_seq')|| ',
				  ''' || v_log_type || ''',
					' || coalesce('''' || v_log_message || '''', 'empty')|| ',
					' || coalesce('''' || v_location || '''', 'null')|| ',
					' || CASE WHEN v_log_type = 'ERROR' THEN TRUE ELSE FALSE END || ',
					current_timestamp,
					current_user);';

EXECUTE v_sql;

RAISE NOTICE 'INSERT SQL IS: %', v_sql; 

 
END;
 





$$
EXECUTE ON ANY;
 

-- Создадим функцию DELTA PARTITION - полная подмена партиций.

 
CREATE OR REPLACE FUNCTION shops.f_load_delta_partition(p_table text, p_ext_table text, 
														  p_partition_key text, p_start_date timestamp, 
														  p_end_date timestamp)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	 


DECLARE


v_temp_table text;
v_table text;
v_ext_table text;
v_params text;
v_dist_key text;
v_where text;
v_start_date date;
v_end_date date;
v_cnt int8;
v_load_interval interval;
v_sql text;
v_result int;
v_table_oid int;

BEGIN

v_table = p_table;
v_ext_table = p_ext_table;
v_temp_table = p_table||'_temp';
v_load_interval = '1 month' ::interval;
v_start_date := date_trunc('month',p_start_date);
v_end_date := date_trunc('month',p_end_date);
v_where = p_partition_key ||' >= ''' ||v_start_date|| '''::date AND '||p_partition_key||' < ''' ||v_end_date|| '''::date';

SELECT c.oid
INTO v_table_oid
FROM pg_class AS c INNER JOIN pg_namespace AS n ON c.relnamespace = n.oid
WHERE n.nspname || '.' ||c.relname = p_table
LIMIT 1;


IF v_table_oid = 0 OR v_table_oid IS NULL THEN
	v_dist_key = 'DISTRIBUTED RANDOMLY';
ELSE 
	v_dist_key = pg_get_table_distributedby(v_table_oid);
END IF;

SELECT COALESCE('with (' || ARRAY_TO_STRING(reloptions, ', ') || ')','')
FROM pg_class
INTO v_params
WHERE oid = p_table::REGCLASS;

 
v_sql = 'DROP TABLE IF EXISTS '|| v_temp_table ||';
		CREATE TABLE '|| v_temp_table ||' (LIKE '|| v_table ||') ' ||v_params||' '||v_dist_key||';';

EXECUTE v_sql;

v_sql = 'INSERT INTO '|| v_temp_table ||' SELECT * FROM '|| v_ext_table ||' WHERE '||v_where;

EXECUTE v_sql;

GET DIAGNOSTICS v_cnt = ROW_COUNT;

RAISE NOTICE 'INSERTED ROWS: %', v_cnt;

v_sql = 'ALTER TABLE '||v_table||' EXCHANGE PARTITION FOR (DATE '''||v_start_date||''') WITH TABLE '||v_temp_table||' WITH VALIDATION';
 
EXECUTE v_sql;

RAISE NOTICE 'EXCHANGE PARTITION: %', v_sql;

EXECUTE 'SELECT COUNT(1) FROM ' || v_table ||' WHERE '|| v_where INTO v_result;

RAISE NOTICE 'EXCHANGE PARTITION: %', v_result; 


v_sql = 'ANALYZE '|| v_table ||';';

EXECUTE v_sql;
 
v_sql = 'DROP TABLE IF EXISTS '|| v_temp_table ||';';

EXECUTE v_sql;

RAISE NOTICE 'TEMPORARY TABLE IS DELETED'; 


PERFORM shops.f_write_log(p_log_type := 'INFO',
						   p_log_message := 'end_of_load_delta_partition',
						   p_location := 'load_delta_partition');

RETURN v_result;


END;


$$

EXECUTE ON ANY;


-- Загрузим данные в таблицы фактов 

SELECT shops.f_load_delta_partition('shops.sales','sales_ext', 'date','2021-03-01','2021-04-01');
 
SELECT shops.f_load_delta_partition('shops.plan','plan_ext', 'date','2021-03-01','2021-04-01');




-- Создадим функцию DELTA UPSERT - предварительное удаление по ключу 
-- и последующая вставка записей из временной таблицы в целевую (альтернативный вариант загрузки)
 
CREATE OR REPLACE FUNCTION shops.f_load_delta_upsert(p_table text, p_ext_table text, p_partition_key text, p_delete_key text, p_start_date timestamp, p_end_date timestamp)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

 
DECLARE
v_temp_table text;
v_table text;
v_ext_table text;
v_params text;
v_dist_key text;
v_delete_key text;
v_where text;
v_where_delete text;
v_start_date date;
v_end_date date;
v_cnt int8;
v_load_interval interval;
v_sql text;
v_result int;
v_table_oid int;
BEGIN

v_table = p_table;
v_ext_table = p_ext_table;
v_temp_table = split_part(p_table,'.',2)||'_test';
v_delete_key = p_delete_key;
v_load_interval = '1 month' ::interval;
v_start_date := date_trunc('month',p_start_date);
v_end_date := date_trunc('month',p_end_date);
v_where = p_partition_key ||' >= ''' ||v_start_date|| '''::date AND '||p_partition_key||' < ''' ||v_end_date|| '''::date';
v_where_delete = v_delete_key ||' >= ''' ||v_start_date|| '''::date AND '||v_delete_key||' < ''' ||v_end_date|| '''::date';

SELECT c.oid
INTO v_table_oid
FROM pg_class AS c INNER JOIN pg_namespace AS n ON c.relnamespace = n.oid
WHERE n.nspname || '.' ||c.relname = p_table
LIMIT 1;


IF v_table_oid = 0 OR v_table_oid IS NULL THEN
	v_dist_key = 'DISTRIBUTED RANDOMLY';
ELSE 
	v_dist_key = pg_get_table_distributedby(v_table_oid);
END IF;

SELECT COALESCE('with (' || ARRAY_TO_STRING(reloptions, ', ') || ')','')
FROM pg_class
INTO v_params
WHERE oid = p_table::REGCLASS;
 
 
v_sql = 'DROP TABLE IF EXISTS '|| v_temp_table ||';
		CREATE TABLE '|| v_temp_table ||' (LIKE '|| v_table ||') ' || v_params ||' '|| v_dist_key ||';';
RAISE NOTICE ' TABLE IS: %', v_sql;

EXECUTE v_sql;

v_sql = 'INSERT INTO '|| v_temp_table ||' SELECT * FROM '|| v_ext_table ||' WHERE '||v_where;
RAISE NOTICE 'TEMP TABLE IS: %', v_sql;
EXECUTE v_sql;

GET DIAGNOSTICS v_cnt = ROW_COUNT;

RAISE NOTICE 'INSERTED ROWS: %', v_cnt;


v_sql = 'DELETE FROM '||v_table||' WHERE '||v_where_delete;

GET DIAGNOSTICS v_cnt = ROW_COUNT;

RAISE NOTICE 'DELETED ROWS: %', v_cnt;
 
EXECUTE v_sql;

RAISE NOTICE 'DELETING ROWS: %', v_sql;


v_sql = 'INSERT INTO '|| v_table ||' SELECT * FROM '|| v_temp_table;

EXECUTE v_sql;

EXECUTE 'SELECT COUNT(1) FROM ' || v_table ||' WHERE '|| v_where INTO v_result;

RAISE NOTICE 'INSERTED ROWS: %', v_result; 

v_sql = 'ANALYZE '|| v_table ||';';

EXECUTE v_sql;

PERFORM shops.f_write_log(p_log_type := 'INFO',
						   p_log_message := 'end_of_load_delta_upsert',
						   p_location := 'load_delta_upsert');


RETURN v_result;


END;
  



$$
EXECUTE ON ANY;

-- Пример запроса для загрузки данных методом "delta_upsert"

SELECT shops.f_load_delta_upsert('shops.plan','plan_ext', 'date', 'date', '2021-03-01','2021-04-01');
 
-- Создадим функцию FULL LOAD - предварительное удаление по ключу и последующая вставка записей из временной таблицы в целевую.
Для таблиц справочников необходимо реализовать FULL загрузку (полная очистка целевой таблицы и полная вставка всех записей).

CREATE OR REPLACE FUNCTION shops.f_full_load(p_table text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

 
DECLARE
v_temp_table text;
v_table text;
v_ext_table text;
v_sql text;
v_cnt int8;
v_result int;

BEGIN
v_table = p_table; 
v_ext_table = p_table||'_ext';
v_temp_table = p_table||'_temp'; 
v_sql = 'TRUNCATE TABLE '|| p_table;

EXECUTE v_sql;
 
v_sql = 'DROP TABLE IF EXISTS '|| v_temp_table ||';
		CREATE TABLE '|| v_temp_table ||' (LIKE '|| v_table ||');';
RAISE NOTICE ' TABLE IS: %', v_sql;

EXECUTE v_sql;

v_sql = 'INSERT INTO '|| v_temp_table ||' SELECT * FROM '|| v_ext_table;
RAISE NOTICE 'TEMP TABLE IS: %', v_sql;
EXECUTE v_sql;

GET DIAGNOSTICS v_cnt = ROW_COUNT;

RAISE NOTICE 'INSERTED ROWS: %', v_cnt;

v_sql = 'INSERT INTO '|| v_table ||' SELECT * FROM '|| v_temp_table;

EXECUTE v_sql;


EXECUTE 'SELECT COUNT(1) FROM ' || v_table INTO v_result;

RAISE NOTICE 'INSERTED ROWS: %', v_result; 

v_sql = 'ANALYZE '|| v_table ||';';

EXECUTE v_sql;

RETURN v_result;


END;
  


$$
EXECUTE ON ANY;

-- Загрузим данные из справочников

SELECT shops.f_full_load('shops.product');
SELECT shops.f_full_load('shops.region');
SELECT shops.f_full_load('shops.chanel');
SELECT shops.f_full_load('shops.price');



select * from shops.product;

--  material|asgrp|brand|matcateg|matdirec|txt 

select * from shops.sales s ;


select * from shops.region;

-- region|txt            |

select * from shops.chanel;
/*
distr_chan|txtsh  |
----------+-------+
1         |Опт    |
2         |Розница|
*/


select * from shops.price;

/*
material|region|distr_chan|price|
--------+------+----------+-----+
  128047|R001  |1         | 7085|
*/
 
 
 
