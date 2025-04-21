 
/*
Создаим пользовательскую функцию для расчёта витрины, 
которая будет содержать результат выполнения плана продаж в разрезе: 
Код "Региона".
Код "Товарного направления" (matdirec).
Код "Канала сбыта".
Плановое количество.
Фактические количество.
Процент выполнения плана за месяц.
Код самого продаваемого товара в регионе.
 
Требования к функции по расчёту витрины:

Функция должна принимать на вход месяц, по которому будут вестись расчеты. 
Таблица должна формироваться в схеме std <номер студента>.
Название таблицы должно формироваться по шаблону plan_fact_<YYYYMM>, где <YYYYMM> - месяц расчета. 
Функция должна иметь возможность безошибочного запуска несколько раз по одному и тому же месяцу. 
  
  
Также создадим представление (VIEW) на созданной ранее витрине со следующим набором полей:

Код "Региона".
Текст "Региона".
Код "Товарного направления" (matdirec).
Код "Канала сбыта".
Текст "Канала сбыта".
Процент выполнения плана за месяц.
Код самого продаваемого товара в регионе.
Код "Бренда" самого продаваемого товара в регионе.
Текст самого продаваемого товара в регионе.
Цена самого продаваемого товара в регионе.

Название представления v_plan_fact.  
*/

 
CREATE OR REPLACE FUNCTION shops.f_mart(p_date varchar)
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
	
	 
DECLARE
v_date varchar;
v_table_name varchar;
v_sql varchar;
v_return int; 
 
BEGIN
v_date := date_trunc('month', to_date(p_date,'YYYYMM'));
v_table_name := 'shops.plan_fact_'||p_date;
 
 
	 
	v_sql = 'DROP TABLE IF EXISTS '||v_table_name||';';

	EXECUTE v_sql;
    
	RAISE NOTICE 'DROPPING TABLE: %', 'DONE';
    
	
		 v_sql = 'CREATE TABLE '||v_table_name||
		   ' WITH (
			appendoptimized=true,
			orientation=column,
			compresslevel=1,
			compresstype=zstd)

		  	AS
			

			with total_sales as(
			SELECT s.region, pd.matdirec, s.distr_chan,sum(s.quantity) as sales_qty
			FROM shops.sales s 
				LEFT JOIN shops.product pd ON s.material = pd.material
			WHERE s.date BETWEEN '''||v_date||'''::timestamp  
				AND ('''||v_date||'''::timestamp  + interval ''1 month'')
			GROUP BY s.region, pd.matdirec, s.distr_chan 
			),
			
			total_plan as(
			
			SELECT p.region, p.matdirec, p.distr_chan, sum(p.quantity) as plan_qty 
			FROM shops.plan p
			WHERE p.date BETWEEN '''||v_date||'''::timestamp  
				AND ('''||v_date||'''::timestamp  + interval ''1 month'')
			GROUP BY p.region, p.matdirec, p.distr_chan 
			),
			
			bestseller as(
			 
			SELECT s.region, s.material, ROW_NUMBER() OVER (PARTITION BY s.region ORDER BY SUM(s.quantity) DESC) AS row_num
			FROM shops.sales s LEFT JOIN shops.product pd ON s.material = pd.material
			WHERE s.date BETWEEN '''||v_date||'''::timestamp  
				AND ('''||v_date||'''::timestamp  + interval ''1 month'') 
			GROUP BY s.region, s.material 
			)  
			
			SELECT tp.region as "Регион", tp.matdirec as "Товарное направление", tp.distr_chan as "Канал сбыта", coalesce(tp.plan_qty,0) as "Плановое количество", 
			ts.sales_qty as "Фактические количество", round(ts.sales_qty::decimal/tp.plan_qty*100,1) as "Процент выполнения плана за месяц", 
			bs.material as "Cамый продаваемый товар в регионе"
			FROM total_plan tp  
				INNER JOIN total_sales ts ON ts.region = tp.region AND ts.matdirec = tp.matdirec AND ts.distr_chan = tp.distr_chan 
				RIGHT JOIN bestseller bs ON tp.region = bs.region and bs.row_num = 1
			ORDER BY tp.region, tp.matdirec, tp.distr_chan
		    DISTRIBUTED RANDOMLY;';

		RAISE NOTICE 'CREATING TABLE: %',v_sql;

		EXECUTE v_sql;

	  
		RAISE NOTICE 'CREATING TABLE: %', 'DONE';
 		
		


		EXECUTE 'SELECT count(*) FROM ' ||v_table_name::text||';' INTO v_return;

        PERFORM shops.f_write_log(p_log_type := 'INFO',
							  p_log_message := v_return || ' rows inserted',
						      p_location := 'Plan Fact Mart calculation');

    	 
	    v_sql =
	    
		'CREATE OR REPLACE VIEW shops.plan_fact  
		 AS 
	 	

		 with total_sales as(
			SELECT s.region, pd.matdirec, s.distr_chan,sum(s.quantity) as sales_qty
			FROM shops.sales s 
				LEFT JOIN shops.product pd ON s.material = pd.material
			WHERE s.date BETWEEN '''||v_date||'''::timestamp  
				AND ('''||v_date||'''::timestamp  + interval ''1 month'')
			GROUP BY s.region, pd.matdirec, s.distr_chan 
			),
			
			total_plan as(
			
			SELECT p.region, p.matdirec, p.distr_chan, sum(p.quantity) as plan_qty 
			FROM shops.plan p
			WHERE p.date BETWEEN '''||v_date||'''::timestamp  
				AND ('''||v_date||'''::timestamp  + interval ''1 month'')
			GROUP BY p.region, p.matdirec, p.distr_chan 
			),
			
			bestseller as(
			 
			SELECT s.region, s.material, ROW_NUMBER() OVER (PARTITION BY s.region ORDER BY SUM(s.quantity) DESC) AS row_num
			FROM shops.sales s LEFT JOIN shops.product pd ON s.material = pd.material
			WHERE s.date BETWEEN '''||v_date||'''::timestamp  
				AND ('''||v_date||'''::timestamp  + interval ''1 month'') 
			GROUP BY s.region, s.material 
			
			)  
			
			SELECT tp.region AS "Код региона",r.txt AS "Текст региона", tp.matdirec as "Товарное направление", tp.distr_chan as "Код канала сбыта", c.txtsh AS "Текст канала сбыта",
			round(ts.sales_qty::decimal/tp.plan_qty*100,1) AS "Процент выполнения плана за месяц", 	bs.material AS "Cамый продаваемый товар в регионе",
			pd.brand::text AS "Код бренда", pd.txt AS "Текст", coalesce(pr.price,0) AS "Цена"
			FROM total_plan tp  
				INNER JOIN total_sales ts ON ts.region = tp.region AND ts.matdirec = tp.matdirec AND ts.distr_chan = tp.distr_chan 
				RIGHT JOIN bestseller bs ON tp.region = bs.region and bs.row_num = 1
				LEFT JOIN shops.product pd ON bs.material = pd.material 
				LEFT JOIN shops.chanel c ON tp.distr_chan = c.distr_chan
				LEFT JOIN shops.region r ON tp.region = r.region
				LEFT JOIN shops.price pr ON bs.material = pr.material AND tp.distr_chan = pr.distr_chan AND tp.region = pr.region
			ORDER BY tp.region, tp.matdirec, tp.distr_chan';			
		
		
		EXECUTE v_sql;
  		
		RAISE NOTICE 'CREATING VIEW: %', 'DONE';


		
		v_sql = 'ANALYZE '|| v_table_name ||';';

		EXECUTE v_sql;

		PERFORM shops.f_write_log(p_log_type := 'INFO',
							  p_log_message := 'End of View Creation',
						      p_location := 'Plan Fact View Creation');
		
		PERFORM shops.f_write_log(p_log_type := 'INFO',
							  p_log_message := 'End of f_load_mart',
						      p_location := 'Plan Fact Mart calculation');
	  
 		RETURN v_return;


END;
 





$$
EXECUTE ON ANY;

 
-- Выполним загрузку тестовых данных за март 2021 г.

select shops.f_mart('202103')
 
 

 
