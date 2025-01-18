# Databricks notebook source
# MAGIC %md
# MAGIC #### **Question 01**

# COMMAND ----------

# MAGIC %md
# MAGIC **How many rows will generate when apply `INNER JOIN, LEFT JOIN, RIGHT JOIN & FULL JOIN` for below tables?**
# MAGIC
# MAGIC      TABLE1   TABLE2
# MAGIC        1        1
# MAGIC        1        2
# MAGIC        2        3
# MAGIC        3        2
# MAGIC        4
# MAGIC        3

# COMMAND ----------

# DBTITLE 1,Create Table tbl_AA
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_AA;
# MAGIC
# MAGIC CREATE TABLE tbl_AA(ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_AA
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1),
# MAGIC (2),
# MAGIC (3),
# MAGIC (4),
# MAGIC (3);
# MAGIC
# MAGIC SELECT * FROM tbl_AA;

# COMMAND ----------

# DBTITLE 1,Create Table tbl_BB
# MAGIC %sql
# MAGIC  DROP TABLE IF EXISTS tbl_BB;
# MAGIC
# MAGIC  CREATE TABLE tbl_BB(ID INT);
# MAGIC
# MAGIC  INSERT INTO tbl_BB
# MAGIC  VALUES
# MAGIC  (1),
# MAGIC  (2),
# MAGIC  (3),
# MAGIC  (2);
# MAGIC
# MAGIC  SELECT * FROM tbl_BB;

# COMMAND ----------

# MAGIC %md
# MAGIC **INNER JOIN**
# MAGIC - An **inner join** returns rows with **matching values** in **both tables**.
# MAGIC - **matching records**:
# MAGIC   - 2(1's) X 1(1's) = 2
# MAGIC   - 1(2's) X 2(2's) = 2
# MAGIC   - 2(3's) X 1(3's) = 2

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AA
# MAGIC INNER JOIN tbl_BB
# MAGIC ON tbl_AA.ID = tbl_BB.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **LEFT JOIN**
# MAGIC - A **left join** returns **all rows** from the **left table** and **matched rows** from the **right table**. **Unmatched rows** from the **right table** result in **NULLs**.
# MAGIC
# MAGIC                                       (or)
# MAGIC - **All records** from **left** table. Only **matching records** from **right** table.
# MAGIC
# MAGIC                                       (or)
# MAGIC - **matching records** and **non-matching** records from the **left table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 1(1's) = 2
# MAGIC     - 1(2's) X 2(2's) = 2
# MAGIC     - 2(3's) X 1(3's) = 2
# MAGIC   - **non-matching records**:
# MAGIC     - 1(4's) = 1 --> ( 4 NULL)

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AA
# MAGIC LEFT JOIN tbl_BB
# MAGIC ON tbl_AA.ID = tbl_BB.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **RIGHT JOIN**
# MAGIC - A **right join** returns **all rows** from the **right table** and **matched rows** from the **left table**. **Unmatched rows** from the **left table** result in **NULLs**.
# MAGIC
# MAGIC                         (or)
# MAGIC - **All records** from **right** table. Only **matching records** from **left** table.
# MAGIC
# MAGIC                         (or)
# MAGIC - **matching records** and **non-matching** records from the **right table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 1(1's) = 2
# MAGIC     - 1(2's) X 2(2's) = 2
# MAGIC     - 2(3's) X 1(3's) = 2
# MAGIC   - **non-matching records**:
# MAGIC     - NA

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AA
# MAGIC RIGHT JOIN tbl_BB
# MAGIC ON tbl_AA.ID = tbl_BB.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **FULL JOIN / FULL OUTER JOIN**
# MAGIC - A **full outer join** returns **all rows** when there's a **match in either table**. **Unmatched rows** from **both tables** result in **NULLs**.
# MAGIC
# MAGIC                                          (or)
# MAGIC - All records from **ALL** tables.
# MAGIC - **matching records**:
# MAGIC     - 2(1's) X 1(1's) = 2
# MAGIC     - 1(2's) X 2(2's) = 2
# MAGIC     - 2(3's) X 1(3's) = 2
# MAGIC
# MAGIC - **non-matching records of Left table**:
# MAGIC     - 1(4's) = 1 --> ( 4 NULL)
# MAGIC
# MAGIC - **non-matching records of Right table**:
# MAGIC     - NA

# COMMAND ----------

# DBTITLE 1,FULL JOIN
# MAGIC %sql
# MAGIC  SELECT * FROM tbl_AA
# MAGIC  FULL OUTER JOIN tbl_BB
# MAGIC  ON tbl_AA.ID = tbl_BB.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **CROSS JOIN**
# MAGIC - A **cross join** returns the **Cartesian product of both tables**, combining **each row from the first table with every row from the second table**.
# MAGIC - 6 x 4 = 24

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AA
# MAGIC CROSS JOIN tbl_BB;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 02**

# COMMAND ----------

# MAGIC %md
# MAGIC **How many rows will generate when apply `INNER JOIN, LEFT JOIN, RIGHT JOIN & FULL JOIN` for below tables?**
# MAGIC
# MAGIC      TAB1   TAB2
# MAGIC        1      1
# MAGIC        2      1
# MAGIC        3      2
# MAGIC        5      4
# MAGIC      NULL   NULL

# COMMAND ----------

# DBTITLE 1,Create Table tbl_A
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_A;
# MAGIC
# MAGIC CREATE TABLE tbl_A
# MAGIC (ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_A
# MAGIC VALUES
# MAGIC (1),
# MAGIC (2),
# MAGIC (3),
# MAGIC (5),
# MAGIC (NULL);
# MAGIC
# MAGIC SELECT * FROM tbl_A;

# COMMAND ----------

# DBTITLE 1,Create Table tbl_B
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_B;
# MAGIC
# MAGIC CREATE TABLE tbl_B
# MAGIC (ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_B
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1),
# MAGIC (2),
# MAGIC (4),
# MAGIC (NULL);
# MAGIC
# MAGIC SELECT * FROM tbl_B;

# COMMAND ----------

# MAGIC %md
# MAGIC **INNER JOIN**
# MAGIC - An **inner join** returns rows with **matching values** in both tables.
# MAGIC - **matching records**:
# MAGIC   - 1(1's) X 2(1's) = 2
# MAGIC   - 1(2's) X 1(2's) = 1

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_A AS A
# MAGIC INNER JOIN tbl_B AS B ON A.ID=B.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **LEFT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **left table**.
# MAGIC   - **matching records**:
# MAGIC     - 1(1's) X 2(1's) = 2
# MAGIC     - 1(2's) X 1(2's) = 1
# MAGIC   - **non-matching records**:
# MAGIC     - 1(3's) = 1 --> (3 NULL)
# MAGIC     - 1(5's) = 1 --> (5 NULL)
# MAGIC     - 1(NULL) = 1 --> (NULL NULL)

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_A AS A
# MAGIC LEFT JOIN tbl_B AS B ON A.ID=B.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **RIGHT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **right table**.
# MAGIC   - **matching records**:
# MAGIC     - 1(1's) X 2(1's) = 2
# MAGIC     - 1(2's) X 1(2's) = 1
# MAGIC   - **non-matching records**:
# MAGIC     - 1(4's) = 1 --> (NULL 4)
# MAGIC     - 1(NULL) = 1 --> (NULL NULL)

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_A AS A
# MAGIC RIGHT JOIN tbl_B AS B ON A.ID=B.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **FULL JOIN / FULL OUTER JOIN**
# MAGIC - **matching records**:
# MAGIC     - 1(1's) X 2(1's) = 2
# MAGIC     - 1(2's) X 1(2's) = 1
# MAGIC
# MAGIC - **non-matching records of Left table**:
# MAGIC     - 1(3's) = 1 --> (3 NULL)
# MAGIC     - 1(5's) = 1 --> (5 NULL)
# MAGIC     - 1(NULL) = 1 --> (NULL NULL)
# MAGIC
# MAGIC - **non-matching records of Right table**:
# MAGIC     - 1(4's) = 1 --> (NULL 4)
# MAGIC     - 1(NULL) = 1 --> (NULL NULL)

# COMMAND ----------

# DBTITLE 1,FULL JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_A AS A
# MAGIC FULL OUTER JOIN tbl_B AS B ON A.ID=B.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **CROSS JOIN**
# MAGIC - A **cross join** returns the **Cartesian product of both tables**, combining **each row from the first table with every row from the second table**.
# MAGIC - 5 x 5 = 25

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_A AS A
# MAGIC CROSS JOIN tbl_B AS B;
