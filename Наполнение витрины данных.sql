/*Создадим пользовательскую функцию для расчёта витрины, 
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
v_return varchar; 
 
BEGIN
v_date := date_trunc('month', to_date(p_date,'YYYYMM'));
v_table_name := concat('shops.plan_fact','_',p_date);
 
 
	DROP TABLE IF EXISTS v_table_name;
	
    
	RAISE NOTICE 'DROPPING TABLE: %', 'DONE';
    
	
		 v_sql = 'CREATE TABLE '||v_table_name||
		   ' WITH (
			appendoptimized=true,
			orientation=column,
			compresslevel=1,
			compresstype=zstd)

		  	AS

			WITH sales_agg AS (

				SELECT s1.region, s1.material, s1.distr_chan, pd1.matdirec, sum(s1.quantity) AS sales_sum 
				FROM shops.sales s1 
				LEFT JOIN shops.product pd1 
					USING(material)
				WHERE s1.date BETWEEN '''||v_date||'''::timestamp  
				AND ('''||v_date||'''::timestamp  + interval ''1 month'')  
				GROUP BY s1.region,s1.material, pd1.matdirec, s1.distr_chan
				),
			
				plan_agg AS (
				
				SELECT p1.region, p1.matdirec,p1.distr_chan, sum(p1.quantity) AS plan_sum 
				FROM shops.plan p1
				WHERE p1.date BETWEEN '''||v_date||'''::timestamp  
				AND ('''||v_date||'''::timestamp  + interval ''1 month'') 
				GROUP BY p1.region, p1.matdirec, p1.distr_chan
				),
			
				max_sales AS (
				SELECT sa.region,sa.matdirec, sa.distr_chan, MAX(sa.sales_sum) AS max_sales
				FROM sales_agg sa 
				GROUP BY sa.region, sa.matdirec, sa.distr_chan
				)
	
 

			SELECT pa.region AS "Код региона", pa.matdirec AS "Товарное направление", pa.distr_chan AS "Код канала сбыта", sa.sales_sum AS "Фактические количество",
			pa.plan_sum AS "Плановое количество", round(sa.sales_sum::decimal / pa.plan_sum * 100,1) AS "Процент выполнения плана за месяц", sa.material AS "Код товара" 
			FROM sales_agg sa
			RIGHT JOIN max_sales ms 
				ON sa.region = ms.region AND sa.sales_sum = ms.max_sales AND sa.distr_chan = ms.distr_chan AND sa.matdirec = ms.matdirec
			INNER JOIN plan_agg pa ON sa.region = pa.region AND sa.distr_chan = pa.distr_chan AND sa.matdirec = pa.matdirec
			ORDER BY "Код региона", "Товарное направление", "Код канала сбыта"
	  		DISTRIBUTED RANDOMLY;';

		RAISE NOTICE 'CREATING TABLE: %',v_sql;

		EXECUTE v_sql;

	  
		RAISE NOTICE 'CREATING TABLE: %', 'DONE';
 		
		SELECT count(*) INTO v_return;

        PERFORM shops.f_write_log(p_log_type := 'INFO',
							  p_log_message := v_return || 'rows inserted',
						      p_location := 'PlanFact Mart calculation');

    	 
	    v_sql =
		'CREATE OR REPLACE VIEW shops.v_plan_fact  
		 AS 
	 		WITH sales_agg AS (

				SELECT s1.region, s1.material, s1.distr_chan, pd1.matdirec, sum(s1.quantity) AS sales_sum 
				FROM shops.sales s1 
				LEFT JOIN shops.product pd1 
					USING(material)
				WHERE s1.date BETWEEN '''||v_date||'''::timestamp  
				AND ('''||v_date||'''::timestamp  + interval ''1 month'')  
				GROUP BY s1.region,s1.material, pd1.matdirec, s1.distr_chan
				),
			
				plan_agg AS (
				
				SELECT p1.region, p1.matdirec,p1.distr_chan, sum(p1.quantity) AS plan_sum 
				FROM shops.plan p1
				WHERE p1.date BETWEEN '''||v_date||'''::timestamp  
				AND ('''||v_date||'''::timestamp  + interval ''1 month'') 
				GROUP BY p1.region, p1.matdirec, p1.distr_chan
				),
			
				max_sales AS (
				SELECT sa.region,sa.matdirec, sa.distr_chan, MAX(sa.sales_sum) AS max_sales
				FROM sales_agg sa 
				GROUP BY sa.region, sa.matdirec, sa.distr_chan
				)
	
 

			SELECT pa.region AS "Код региона", r.txt AS "Текст региона", pa.matdirec AS "Товарное направление", pa.distr_chan AS "Код канала сбыта", c.txtsh AS "Текст канала сбыта",
			sa.sales_sum AS "Фактические количество", pa.plan_sum AS "Плановое количество", round(sa.sales_sum::decimal / pa.plan_sum * 100,1) AS "Процент выполнения плана за месяц", 
			sa.material AS "Код товара", pd.brand AS "Код бренда", pd.txt AS "Текст", coalesce(pr.price,0) AS "Цена"
			FROM sales_agg sa
			RIGHT JOIN max_sales ms 
				ON sa.region = ms.region AND sa.sales_sum = ms.max_sales AND sa.distr_chan = ms.distr_chan AND sa.matdirec = ms.matdirec
			INNER JOIN plan_agg pa ON sa.region = pa.region AND sa.distr_chan = pa.distr_chan AND sa.matdirec = pa.matdirec
			LEFT JOIN shops.product pd ON sa.matdirec = pd.matdirec AND sa.material = pd.material
			LEFT JOIN shops.chanel c ON pa.distr_chan = c.distr_chan
			LEFT JOIN shops.region r ON pa.region = r.region
			LEFT JOIN shops.price pr ON sa.material = pr.material AND sa.region = pr.region AND sa.distr_chan = pr.distr_chan
			ORDER BY "Код региона";';
		
		
		EXECUTE v_sql;
  		
		RAISE NOTICE 'CREATING VIEW: %', 'DONE';

		PERFORM shops.f_write_log(p_log_type := 'INFO',
							  p_log_message := 'End of PlanFact View Creation',
						      p_location := 'PlanFact View Creation');
		
		PERFORM shops.f_write_log(p_log_type := 'INFO',
							  p_log_message := 'End of mart loading',
						      p_location := 'PlanFact Mart calculation');
	  
 		RETURN v_return;


END;
 





$$
EXECUTE ON ANY;
 

select shops.f_mart('202103')

 