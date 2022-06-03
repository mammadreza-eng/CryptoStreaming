from typing import Final
import requests as rq
import uuid
import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from PyBrif import pybrief

BINANCE_TICKER: Final[str] = "https://www.binance.com/api/v3/ticker/price"
LOG_PATH: Final[str] = r"C:\\CryptoLog\\"
API_INTERVAL_SEC: Final[int] = 3
STREAM_INTERVAL_SEC: Final[int] = 6

class StreamREST(pybrief.Thread):
    def __init__(self, url: str, intervalSec: int=1, savePath: str="", ext: str=".json") -> None:
        super(StreamREST, self).__init__(intervalSec)
        self.__url = url
        self.__savePath = savePath
        self.__ext = ext

    def getFilename(self) -> str:
        return self.__savePath + str(uuid.uuid4()) + self.__ext
    
    def getRequest(self) -> str:
        return rq.get(self.__url).text
    
    def save(self):
        with open(self.getFilename(), "w") as f:
            f.write(self.getRequest())
            f.close

    def execute(self):
        self.save()

priceCollector = StreamREST(url=BINANCE_TICKER, savePath=LOG_PATH, intervalSec=API_INTERVAL_SEC)
priceCollector.start()

spark = SparkSession.builder.getOrCreate()
print('Spark Version: ' + spark.version)

tickerSchema = T.StructType([T.StructField("price", T.StringType(), True),T.StructField("symbol", T.StringType(), True)])
tickerStream = spark.readStream.schema(tickerSchema).option("maxFilePerTrigger", 1).json(LOG_PATH)

tickerStream = tickerStream.filter(tickerStream.symbol.isin(["BTCUSDT", "DOGEUSDT", "ICPUSDT", "AXSUSDT", "SOLUSDT"]))
print('isStreaming --> ', tickerStream.isStreaming)
streamingQuery = tickerStream.writeStream.queryName("vwTicker").format("memory").outputMode("append")

streamingQuery.start()

class SparkStream(pybrief.Thread):
    def execute(self):
        _df = spark.sql("""SELECT symbol, MAX(price) price, COUNT(*) AS countAll
                             FROM vwTicker
                            WHERE symbol IN ('BTCUSDT', 'ICPUSDT')
                            GROUP BY symbol""")
        
        print("Count --> ", _df.count())
        if (_df.count() > 0):
            _df.show(10)

checkStreamedData = SparkStream(intervalSec=STREAM_INTERVAL_SEC)
checkStreamedData.start()

time.sleep(60) # Keep the app running to see the result for 1 min

priceCollector.kill()
checkStreamedData.kill()