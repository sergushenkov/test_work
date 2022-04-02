/*
Тренажер по ссылке открывается, но онлайн выполнение запросов без оплаты недоступно.
В связи с этим задание выполнял в БД Postgres v.11.6 - возможно что выполнение на другой версии или диалекте потребует
незначительных изменений в коде.
*/

select version();  -- PostgreSQL 11.6

/*
По исходноый ссылке даны данные только одной из таблиц (emplyees) - в сети удалось найти распечатки содержимого двух
других (salary_grade и department). С помощью скрипта на питон (gen_insert.py) создал команды для заполнения всех трёх таблиц.
Команды для создания таблиц создал вручную на основе ER-диаграмм онлайн-тренажера
*/

-- генерация исходных данных

DROP TABLE if exists public.salary_grade;
DROP TABLE if exists public.employees;
DROP TABLE if exists  public.department;

CREATE TABLE if not exists public.salary_grade (
grade int4 NOT NULL,
min_salary int4 NULL,
max_salary int4 NULL,
CONSTRAINT salary_grade_pkey PRIMARY KEY (grade)
);

CREATE TABLE if not exists public.department (
dep_id int4 NOT NULL,
dep_name varchar(20) NULL,
dep_location varchar(15) NULL,
CONSTRAINT department_pkey PRIMARY KEY (dep_id)
);

CREATE TABLE if not exists public.employees (
emp_id int4 NOT NULL,
emp_name varchar(15) NULL,
job_name varchar(10) NULL,
manager_id int4 NULL,
hire_date date NULL,
salary numeric(10, 2) NULL,
commission numeric(7, 2) NULL,
dep_id int4 NULL,
CONSTRAINT employees_pkey PRIMARY KEY (emp_id),
CONSTRAINT employees_dep_id_fkey FOREIGN KEY (dep_id) REFERENCES public.department(dep_id)
);

INSERT INTO salary_grade(grade, min_salary, max_salary) VALUES
(1, 800, 1300),
(2, 1301, 1500),
(3, 1501, 2100),
(4, 2101, 3100),
(5, 3101, 9999);

INSERT INTO department(dep_id, dep_name, dep_location) VALUES
(1001, 'FINANCE', 'SYDNEY'),
(2001, 'AUDIT', 'MELBOURNE'),
(3001, 'MARKETING', 'PERTH'),
(4001, 'PRODUCTION', 'BRISBANE');

INSERT INTO employees(emp_id, emp_name, job_name, manager_id, hire_date, salary, commission, dep_id) VALUES
(68319, 'KAYLING', 'PRESIDENT', null, '1991-11-18', '6000.00', null, 1001),
(66928, 'BLAZE', 'MANAGER', 68319, '1991-05-01', '2750.00', null, 3001),
(67832, 'CLARE', 'MANAGER', 68319, '1991-06-09', '2550.00', null, 1001),
(65646, 'JONAS', 'MANAGER', 68319, '1991-04-02', '2957.00', null, 2001),
(67858, 'SCARLET', 'ANALYST', 65646, '1997-04-19', '3100.00', null, 2001),
(69062, 'FRANK', 'ANALYST', 65646, '1991-12-03', '3100.00', null, 2001),
(63679, 'SANDRINE', 'CLERK', 69062, '1990-12-18', '900.00', null, 2001),
(64989, 'ADELYN', 'SALESMAN', 66928, '1991-02-20', '1700.00', '400.00', 3001),
(65271, 'WADE', 'SALESMAN', 66928, '1991-02-22', '1350.00', '600.00', 3001),
(66564, 'MADDEN', 'SALESMAN', 66928, '1991-09-28', '1350.00', '1500.00', 3001),
(68454, 'TUCKER', 'SALESMAN', 66928, '1991-09-08', '1600.00', '0.00', 3001),
(68736, 'ADNRES', 'CLERK', 67858, '1997-05-23', '1200.00', null, 2001),
(69000, 'JULIUS', 'CLERK', 66928, '1991-12-03', '1050.00', null, 3001),
(69324, 'MARKER', 'CLERK', 67832, '1992-01-23', '1400.00', null, 1001);


-------------------
-- РЕШЕНИЕ ЗАДАЧ --
-------------------

/*
1. Вывести список сотрудников, получающих заработную плату большую чем у непосредственного руководителя.
   Отразить поля: имя, должность, id отдела, заработная плата сотрудника, зарплата руководителя

emp_name|job_name|dep_id|salary |manager_salary|
--------+--------+------+-------+--------------+
SCARLET |ANALYST |  2001|3100.00|       2957.00|
FRANK   |ANALYST |  2001|3100.00|       2957.00|

*/

select e.emp_name
     , e.job_name
     , e.dep_id
     , e.salary
     , m.salary as manager_salary
  from employees e
  join employees m
    on e.manager_id = m.emp_id
 where e.salary > m.salary
;

/*
2. Вывести список сотрудников, получающих минимальную заработную плату в своем отделе.
   Отразить поля: имя, должность, id отдела, заработная плата сотрудника

emp_name|job_name|dep_id|salary |
--------+--------+------+-------+
SANDRINE|CLERK   |  2001| 900.00|
JULIUS  |CLERK   |  3001|1050.00|
MARKER  |CLERK   |  1001|1400.00|

*/

select e.emp_name
     , e.job_name
     , e.dep_id
     , e.salary
  from employees e
  join (
        select dep_id
             , min(salary) as min_salary
          from employees
         group by dep_id
       ) m
    on e.dep_id = m.dep_id
   and e.salary = m.min_salary
;

/*
3. Вывести список ID отделов, количество сотрудников в которых превышает 3 человека

dep_id|
------+
  3001|
  2001|

*/

select dep_id
  from employees
 group by dep_id
having count(*) > 3
;

/*
4. Вывести список сотрудников, не имеющих назначенного руководителя, работающего в том же
   отделе. Отразить поля: имя, должность, наименование отдела

emp_name|job_name |dep_id|
--------+---------+------+
KAYLING |PRESIDENT|  1001|
BLAZE   |MANAGER  |  3001|
JONAS   |MANAGER  |  2001|

*/

select e.emp_name
     , e.job_name
     , e.dep_id
  from employees e
  left join employees m
    on e.manager_id = m.emp_id
   and e.dep_id = m.dep_id
 where m.emp_id is null
;

/*
5. Ранжировать сотрудников в каждом отделе по стажу работы в днях до текущей даты
   (current_date) по убывающей. Отразить поля: имя, должность, id отдела, стаж в днях, ранг

emp_name|job_name |dep_id|day_on_work|rank|
--------+---------+------+-----------+----+
SANDRINE|CLERK    |  2001|      11427|   1|
ADELYN  |SALESMAN |  3001|      11363|   2|
WADE    |SALESMAN |  3001|      11361|   3|
JONAS   |MANAGER  |  2001|      11322|   4|
BLAZE   |MANAGER  |  3001|      11293|   5|
CLARE   |MANAGER  |  1001|      11254|   6|
TUCKER  |SALESMAN |  3001|      11163|   7|
MADDEN  |SALESMAN |  3001|      11143|   8|
KAYLING |PRESIDENT|  1001|      11092|   9|
FRANK   |ANALYST  |  2001|      11077|  10|
JULIUS  |CLERK    |  3001|      11077|  10|
MARKER  |CLERK    |  1001|      11026|  12|
SCARLET |ANALYST  |  2001|       9113|  13|
ADNRES  |CLERK    |  2001|       9079|  14|

Примечание - так как явно не прописано, как рассчитывать ранг, использовал вариант,
когда работники с одинаковым стажем имеют одинаковый ранг, а следующий после них работник,
будет иметь ранг не на единицу больше, а через пропуск.
*/

select emp_name
     , job_name
     , dep_id
     , (current_date - hire_date) as day_on_work
     , rank() over(order by (current_date - hire_date) desc)
  from employees e
 order by (current_date - hire_date) desc
;

/*
6. Определить количество сотрудников, относящихся к каждому уровню заработной платы, отсортировать по убыванию

grade|count_employees|
-----+---------------+
    4|              5|
    1|              3|
    2|              3|
    3|              2|
    5|              1|

Примечание - из формулировки не ясно, сортировать по убыванию грейда или по убыванию количества работников.
Предположил что по количеству работников.
*/

select grade
     , count(*) as count_employees
  from salary_grade
  join employees
    on salary between min_salary and max_salary
 group by grade
 order by count(*) desc
;


-- очистка базы данных
DROP TABLE if exists public.salary_grade;
DROP TABLE if exists public.employees;
DROP TABLE if exists  public.department;