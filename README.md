# BEELINE Test Work

## SQL
[Текст задания по sql](sql/task_of_sql.txt)

[Решение](sql/solutions.sql) - на PostgreSQL 11.6

## Spark
[Текст задания по Spark](task_of_spark.txt)

[Решение](beeline_test_work.py)

Задание было рекомендовано выполнить на Scala - не смог по смешной причине: 
не удалось добиться чтобы работали вместе IDEA + Spark + sbt-assembly. 
До этого Scala + Spark запускал только на рабочем кластере, не вникая в детали окружения.
В итоге - решил на PySpark.

В решении использовал следующие допущения:
* Любимый продукт выводится для всех клиентов, независимо от статуса клиентов
* Для определения любимого продукта используются только заказы со статусом delivered
* В качестве любимого продукта выдаётся тот продукт, который клиент заказывал в самом большом количестве.
  Как вариант - можно было бы выбрать тот, на который он потратил больше всего денег.
  Чтобы использовать этот вариант необходимо раскомментировать строки с 67 по 74 в коде - это меняет результат
* Spark сохраняет файл в отдельную директорию, даже при локальном запуске. 
  Тут реализовано перемещение файла в директорию resources. 
  При запуске на hadoop этот участок кода не будет работать (строки 100..104)
