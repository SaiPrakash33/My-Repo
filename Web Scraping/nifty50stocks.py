# Code for web scrapping to get top 50 stocks details
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup

my_url=  "https://www.tickertape.in/indices/nifty-50-index-.NSEI/constituents?type=marketcap"
uClient= uReq(my_url)
page_html= uClient.read()
uClient.close()

page_soup=soup(page_html,"html.parser")
page_soup

containers= page_soup.findAll("div", {"class": "constituent-data-row-holder"})
print(len(containers))

filename= "nifty50stocks.csv"
f= open(filename, "w")

headers= "Stock name, Stock Alias, Index Weight, Free Float MarketCap(CR) \n"
f.write(headers)

for container in containers:
    stocks_mov= container.findAll("a", {"class": "typography-body-medium-m"})
    stocks = stocks_mov[0].text
    stockAlias_mov = container.findAll("span", {"class": "ticker"})
    stockAlias = stockAlias_mov[0].text
    weight_mov = container.findAll("span", {"class": "numeric"})
    weightage = weight_mov[2].text
    marketcap_mov = container.findAll("span", {"class": "numeric"})
    marketcap = marketcap_mov[3].text
  
    f.write(stocks + "," +stockAlias+ "," + weightage + "," + marketcap  + "\n")
    
f.close()

# If pyspark is not present in your system use the below code to install the Pyspark, Even if you are using google colab use below code to install Pyspark
# !pip install pyspark

from pyspark.sql import SparkSession
# If you are using google colab use the below code to create a sparksession
spark = SparkSession.builder.master("local").appName("Colab").config('spark.ui.port', '4050').getOrCreate()
# SparkSession is an entry point to underlying Spark functionality. All functionality available with SparkContext is also available in SparkSession. Also, it provides APIs to work on DataFrames and Datasets.
# If you are using your local System then use below line of code to create a SparkSession
# spark = SparkSession.builder.master('local[1]').appName('SparkByExamples.com').getOrCreate()

df = spark.read.csv("nifty50stocks.csv", header=True)  
df.show()


# Fetching Dividend data
import requests

baseUrl = "https://api.tickertape.in/indices/corporates/dividends/.NSEI?count=5&offset={}"
page_num1 = 222

filename1= "nifty50StocksDividends.csv"
f1= open(filename1, "w")

headers= "Stock Alias, Dividend Amount, ExDate \n"
f1.write(headers)

stockAlias = []
amount = []
exDate = []

while True:
  if page_num1 == 1:
    page_num1 = page_num1+1
  response = requests.get(baseUrl.format(page_num1))
  data = response.json()
  d1 = data["data"]
  d = d1["past"]
  for i in d:
    for key,value in i.items():
      if key == "ticker":
        stockAlias.append(value.strip())
      if key == "value":
        amount.append(value.strip())
      if key == "exDate":
        exDate.append(value.strip())
  if page_num1 < 498:
    page_num1 = page_num1+1
  else:
    break

n = len(exDate)
for i in range(0,n):
  f1.write(stockAlias[i] + "," + amount[i] + "," + exDate[i]  + "\n")

f1.close()

# To display the data in the created file as Dataframe  
df1 = spark.read.csv("nifty50StocksDividends.csv", header=True)  
df1.show()

# To remove the duplicates
nifty50stocksDividens1 = df1.dropDuplicates()
nifty50stocksDividens1.show(10)

# To save the Dataframe as a cvs file 
f1= open(filename1, "w")
nifty50stocksDividens1.write.format("csv").mode("overwrite").options(header=True).save("/content/sample_data")
f1.close()