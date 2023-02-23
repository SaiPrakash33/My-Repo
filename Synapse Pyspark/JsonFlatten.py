# Flattening any Structured or unstructured JSON file.

from pyspark.sql.functions import explode
feed_df = spark.read.json('abfss://sharedfs@shareddls.dfs.core.windows.net/SampleMultiRecord.json', multiLine=True)
#  Before executing the above line of code in your synapse, Upload the SampleMultiRecord json file provided in the same folder to your primary Synapse storage account.
feed_df.printSchema()
display(feed_df)

from pyspark.sql.functions import udf, explode_outer, col
from pyspark.sql.types import MapType,StringType, ArrayType, StructType
#from pyspark.sql.functions import lit,col,explode_outer,concat,to_json,struct,expr,from_json, udf
def get_complex_fields(df, block_list):
   # compute Complex Fields (Lists and Structs) in Schema   
   return dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if field.name not in block_list and type(field.dataType) == ArrayType or  type(field.dataType) == StructType])

#Flatten array of structs and structs
def flatten_dataframe(df,deny_list=[]):
   cast_str = udf(lambda x : str(x), StringType())
   complex_fields = get_complex_fields(df, deny_list)
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      expanded = []
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         for k in [ f"{n.name}|{n.dataType}" for n in  complex_fields[col_name]]:
           field_name = k.split('|')[0]
           field_dataType = k.split('|')[1].split("(")[0]
           if field_dataType not in ("StringType","ArrayType","StructType"):
             expanded.append(cast_str(col(col_name+'.'+field_name)).alias(field_name))
           else:
             expanded.append(col(col_name+'.'+field_name).alias(field_name))
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.explode_outer.html
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))

      # recompute remaining Complex Fields in Schema       
      complex_fields = get_complex_fields(df, deny_list)
   return df

feed_df0 = flatten_dataframe(feed_df)
display(feed_df0)