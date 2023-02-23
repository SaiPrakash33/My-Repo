## Converting JSON to DF in Azure Synapse.
from pyspark.sql.functions import explode
df1 = spark.read.json('abfss://sharedfs@shareddls.dfs.core.windows.net/EmployeeData.json', multiLine=True)
# Before executing the above line of code in your synapse, Upload the EmployeeData json file provided in the same folder to your primary Synapse storage account.
df1.printSchema()
df1 = df1.withColumn("Certs",explode('certs.certName')).select('name','Id','IsActive','Unit','Certs')
df1.show()
df1.printSchema()

## Adding new rows 
values = [('ABCDName',231,'True','STG','DP203'), ('ABCDName',123,'True','DNA','DP203'), ('PQRSName',123,'True','DNA','DP203'), ('XYZName',123,'True','DNA','DP503')]
columns = ['name','Id','IsActive','Unit','Certs']
df2 = spark.createDataFrame(values, columns)
df2.show()
df2.printSchema()

df1 = df1.union(df2)
df1.show()

# To create pivot
pivot_df = df1.groupBy("Id").pivot("certs").count().show()

# To create row_number for the df
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("Id").orderBy("certs")
df1.withColumn("Row_number",row_number().over(windowSpec)).show()

# To create a rank coloumn to the df
from pyspark.sql.functions import rank
df1.withColumn("rank",rank().over(windowSpec)).show()

# To create dense rank to the df
from pyspark.sql.functions import dense_rank
df1.withColumn("dense_rank",dense_rank().over(windowSpec)).show()

# To save the df as a table
# df1.write.format("delta").mode("overwrite").saveAsTable("SampleEmployeeDetails")
