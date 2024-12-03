# Databricks notebook source
# MAGIC %md
# MAGIC ## Read CSV file

# COMMAND ----------

df = spark.read.format('csv').option('inferschema', True).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')
df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition - DDL

# COMMAND ----------

my_schema = '''
                Item_Identifier STRING,
                Item_Weight STRING,
                Item_Fat_Content STRING,
                Item_Visibility double,
                Item_Type STRING,
                Item_MRP double,
                Outlet_Identifier string,
                Outlet_Establishment_Year integer,
                Outlet_Size string,
                Outlet_Location_Type string,
                Outlet_Type string,
                Item_Outlet_Sales double
            '''

# COMMAND ----------

df = spark.read.format('csv').schema(my_schema).option('header', True)\
                .load('/FileStore/tables/BigMart_Sales.csv')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Difference between DDL and StructType schema definitions
# MAGIC
# MAGIC ###    Feature	           DDL	         StructType
# MAGIC - Definition Type	String-based	Programmatic (using classes)
# MAGIC - Complexity	Simple, for flat schemas	Supports nested and complex structures
# MAGIC - Nullability and Metadata	Cannot specify	Can specify explicitly
# MAGIC - Readability	Concise and SQL-like	Verbose but more descriptive
# MAGIC - Usage Scenario	Quick setups, simple cases	Detailed schemas, advanced control

# COMMAND ----------

# MAGIC %md
# MAGIC #### When to Use Which?
# MAGIC - DDL: Use when you need a quick, simple, and readable schema definition, especially if dealing with flat data.
# MAGIC - StructType: Use when you need precise control over the schema, have nested data, or require attributes like nullability and metadata.
# MAGIC
# MAGIC
# MAGIC If your schema is static and straightforward, DDL is sufficient. For dynamic, complex data transformations, StructType is the better choice.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT

# COMMAND ----------

df_sel = df.select('Item_Identifier','Item_MRP').display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Mutiple columns in select

# COMMAND ----------

df_col = df.select(col("Item_Identifier"), col("Item_Type"))

df_alias = df_col.select(col("Item_Identifier").alias('Item ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter/Where
# MAGIC when using two conditions in single statement, encapsulate each condition within braces, for spark to understand them as two different conditions

# COMMAND ----------

df_filter1 = df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

df_filter2 = df.filter((col('Item_type') == 'Soft Drinks') & (col('Item_Weight') < 10)).display()

# COMMAND ----------

df_filter3 = df.filter((col('Outlet_Location_Type').isin('Tier 1','Tier 2')) & (col('Outlet_Size').isNull())).display()

# COMMAND ----------


df.withColumnRenamed('Item_Weight', 'Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn
# MAGIC
# MAGIC **Scenario 1**: create a new column, either we can give constant value or use data from different columns to give value to the new column
# MAGIC
# MAGIC **Scenario 2**: modify an existing column. Use regexp_replace function and inputs are col name, old value and new value

# COMMAND ----------

df_newcolumn = df.withColumn('wt_mrp', (col('Item_Weight')) * (col('Item_Mrp')) ).display()

# COMMAND ----------

df_modifycolumn = df.withColumn('Item_Fat_Content', regexp_replace('Item_Fat_Content', "Regular", 'RF'))\
                        .withColumn('Item_Fat_Content', regexp_replace('Item_Fat_Content', "Low Fat", 'LF')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting
# MAGIC
# MAGIC use cast() function and pass the required type of datatype.

# COMMAND ----------

df_typecast = df.withColumn('Item_weight', col('Item_weight').cast(StringType()) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort/OrderBy

# COMMAND ----------

df_sort = df.sort(col("Item_weight").asc()).display()
              

# COMMAND ----------

### Adding in another condition to filter items without null


df_sort = df.sort(col("Item_weight").asc())\
              .where(col('Item_weight').isNotNull()).display()
              #.where(col('Item_weight') != 'Null').display()

# COMMAND ----------

### Multiple Columns sorting

df_multiplesort = df.sort(['Item_Weight','Item_Visibility'], ascending = [0,0]).display()

df_multiplesort = df.sort(['Item_Weight','Item_Visibility'], ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop

# COMMAND ----------

df.drop(col('Item_Visibility')).display()

df.drop('Item_Visibility', 'Outlet_Identifier').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates

# COMMAND ----------

# Scenario 1 of removing entire duplicate row


df.dropDuplicates().display()

# COMMAND ----------

## Drop duplicates in particular column

df.dropDuplicates(subset = ['Item_Type'])\
        .sort(col('Item_Type').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union & UnionByName
# MAGIC
# MAGIC
# MAGIC unionByName function with allowMissingColumns argument gives more flexibility interms of avoiding errors if there is a mismatch in data type of the column between different dataframes

# COMMAND ----------

data1 = [('1','A'), ('1','b')]
schema1 = 'id STRING', 'name String'

df1 = spark.createDataFrame(data1, schema1)


data2 = [('3','C'), ('4','d')]
schema2 = 'id STRING', 'name String'

df2 = spark.createDataFrame(data2, schema2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data3 = [('E','5'), ('F','6')]
schema3 = 'name String', 'id STRING'

df3 = spark.createDataFrame(data3, schema3)

# COMMAND ----------

df2.unionByName(df3).display()

# COMMAND ----------


data5 = [('E','5', 1), ('F','6', 1)]
schema5 = 'name String', 'id STRING', 'id1 INTEGER'

df5 = spark.createDataFrame(data5, schema5)

data4 = [('E','5', 'X'), ('F','6', 'X')]
schema4 = 'name String', 'id STRING', 'letter STRING'

df4 = spark.createDataFrame(data4, schema4)

# COMMAND ----------

df4.unionByName(df5, allowMissingColumns=True).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String Functions

# COMMAND ----------

df.select(initcap('item_type')).display()  # camel case 

df.select(lower('item_type')).display()

df.select(upper('item_type')).display()

df.select(upper('item_type').alias('upper_case')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('substringexample', substring('item_identifier',1,3))\
    .select(col('substringexample'), col('item_identifier')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Functions

# COMMAND ----------

df_dateexample = df.withColumn('curr_date', current_date())\
    .withColumn('oneweekahead', date_add('curr_date',7))\
        .withColumn('oneweekbehind', date_sub('curr_date',7))

# COMMAND ----------

df_dateexample.withColumn('date_diff', datediff('oneweekahead','curr_date')).display()

# COMMAND ----------

df_dateexample.withColumn('year', year('oneweekahead')).display()

# COMMAND ----------

df_dateexample.withColumn('changeformat', date_format('oneweekahead', 'dd-MM-yyyy')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null Handling
# MAGIC
# MAGIC - dropna('all')  -- drops the record which has all columns as NULL
# MAGIC - dropna('any')  -- drops the record even when one column is NULL
# MAGIC - dropna(subset=['column1', 'column2'])  -- drops null only when a parituclar column has Null value

# COMMAND ----------

df.dropna('any').display()

df.dropna('all').display()

df.dropna(subset = ['item_weight']).display()


# COMMAND ----------

# MAGIC %md
# MAGIC - fillna('text') -- fills with given values for all NULL cells
# MAGIC - fillna('text', subset = ['column name']) -- fills on specific column

# COMMAND ----------

df.fillna('not available').display()

df.fillna('0', subset = ['item_weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split and Indexing

# COMMAND ----------

df.withColumn('Outlet_Type', split('Outlet_Type', ' ')).display()

df.withColumn('Outlet_Type', split('Outlet_Type', ' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode

# COMMAND ----------

df_explode = df.withColumn('outlet_type', split('outlet_type',' '))

df_explode.withColumn('outlet_type', explode('outlet_type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Array Contains

# COMMAND ----------

df_explode.withColumn('type1_flag', array_contains('outlet_type', 'Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GroupBy

# COMMAND ----------

df.groupBy('Item_Type').sum('Item_MRP').display()

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

df.groupBy('Item_Type', 'outlet_size').agg(sum('Item_MRP').alias('grouped data')).display()

df.groupBy('Item_Type', 'outlet_size').agg(sum('Item_MRP').alias('sum'), avg('Item_MRP').alias('average')).display()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect List

# COMMAND ----------

data = [
        ('user1', 'book1'),
        ('user1', 'book2'),
        ('user2', 'book2'),
        ('user3', 'book3'),
        ('user3', 'book1'),
]

schema = 'user string', 'book string'

df_data = spark.createDataFrame(data, schema)

# COMMAND ----------

df_data.groupBy('user string').agg(collect_list('book string')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot
# MAGIC
# MAGIC - GroupBy will contain the row details
# MAGIC - pivot will have the columns required
# MAGIC - agg will have the column on which we need to do calculations

# COMMAND ----------

df.groupBy('item_type').pivot('outlet_size').agg(sum('item_mrp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### When-Otherwise
# MAGIC
# MAGIC Similar to Case when in SQL

# COMMAND ----------

# Scenario 1

df_flag = df.withColumn('diet_flag', when(col('item_type')=='Meat', 'Non-Veg').otherwise('Veg'))

# COMMAND ----------

# Scenario 2

df_flag.withColumn(
    'veg_expensive_flag',
        when(((col("diet_flag")== 'Veg') & (col("Item_MRP") > 100)), 'veg_expensive')\
          .when((col("diet_flag")== 'Veg') & (col("Item_MRP") < 100), 'veg_inexpensive')\
              .otherwise('non-veg')
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### JOINS

# COMMAND ----------


dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

# MAGIC %md
# MAGIC when its required to select multiple columns from different DF, don't use col function. use it via direct reference

# COMMAND ----------

## Inner

df1.join(df2, df1["dept_id"] == df2['dept_id'], "inner")\
    .select(
        df1["emp_id"], 
        df1["emp_name"], 
        df1["dept_id"], 
        df2["department"]
    ).display()

# COMMAND ----------

## Left

df1.join(df2, df1["dept_id"] == df2['dept_id'], "left").display()

# COMMAND ----------

## Right

df1.join(df2, df1["dept_id"] == df2['dept_id'], "Right").display()

# COMMAND ----------

## Anti Join -- Provides the data present only in 1st table and not in second table

df1.join(df2, df1["dept_id"] == df2['dept_id'], "anti").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Functions
# MAGIC
# MAGIC - row_number()
# MAGIC - rank()
# MAGIC - dense_rank()

# COMMAND ----------

from pyspark.sql.window import *

# COMMAND ----------

df.withColumn('rowID', row_number().over(Window.orderBy('item_identifier'))).display()

# COMMAND ----------

df.withColumn('rank', rank().over(Window.orderBy('item_identifier'))).display()

# COMMAND ----------

df.withColumn('denserank', dense_rank().over(Window.orderBy(col('item_identifier').asc())))\
    .withColumn('rank', rank().over(Window.orderBy('item_identifier'))).display() 

# COMMAND ----------

df.withColumn('cum_sum', sum("Item_MRP").over(Window.orderBy('item_type').rowsBetween(Window.unboundedPreceding, Window.currentRow)))\
    .withColumn('tot_sum', sum("Item_MRP").over(Window.orderBy('item_type').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).display()

# COMMAND ----------

window_spec = Window.partitionBy('Item_Type').orderBy('Item_MRP').rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn('cum_sum', sum("Item_MRP").over(window_spec)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Defined Functions
# MAGIC
# MAGIC
# MAGIC Executor is a JVM. So any UDF created should go through a Python interpretor, send to JVM and comes back. So performance takes a hit

# COMMAND ----------

## Create and execute an UDF

def func_name(x):
    return x * x

func_name(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Regiter UDF as a pySpark function

# COMMAND ----------

# step 1: create the logic for the function
def func_name(x):
    return x * x

# step 2: register it as a pySpark function
my_udf = udf(func_name)

# ste 3: use the function
df.withColumn('udftest', my_udf('item_mrp')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Write the data
# MAGIC
# MAGIC - Append
# MAGIC - Overwrite
# MAGIC - Error -- if the file is already present, errors out
# MAGIC - Ignore -- if file exists, no error but doesn't write any data

# COMMAND ----------

df.write.format('csv').save('/FileStore/tables/CSV/data.csv')

df.write.format('csv').option('path', '/FileStore/tables/CSV/data1.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Different Write Modes

# COMMAND ----------

df.write.format('csv').mode('append').option('path', '/FileStore/tables/CSV/data1.csv').save()

# COMMAND ----------

df.write.format('csv').mode('overwrite').option('path', '/FileStore/tables/CSV/data1.csv').save()

# COMMAND ----------

df.write.format('csv').mode('error').option('path', '/FileStore/tables/CSV/data3.csv').save()

# COMMAND ----------

df.write.format('csv').mode('ignore').option('path', '/FileStore/tables/CSV/data2.csv').save()
