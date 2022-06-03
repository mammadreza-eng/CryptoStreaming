<div class="cell markdown">

Load libraries

</div>

<div class="cell code" data-execution_count="1">

``` python
from typing import Final
import requests as rq
import uuid
import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from PyBrif import pybrief
```

</div>

<div class="cell markdown">

Initialize

</div>

<div class="cell code" data-execution_count="2">

``` python
BINANCE_TICKER: Final[str] = "https://www.binance.com/api/v3/ticker/price"
LOG_PATH: Final[str] = r"C:\\CryptoLog\\"
API_INTERVAL_SEC: Final[int] = 3
STREAM_INTERVAL_SEC: Final[int] = 6
```

</div>

<div class="cell markdown">

Create a thread to get and save all price as the producer

</div>

<div class="cell code" data-execution_count="3">

``` python
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
```

</div>

<div class="cell markdown">

Fire Spark and create a Spark Session

</div>

<div class="cell code" data-execution_count="4">

``` python
spark = SparkSession.builder.getOrCreate()
print('Spark Version: ' + spark.version)
```

<div class="output stream stdout">

    Spark Version: 3.2.1

</div>

</div>

<div class="cell markdown">

Set corresponding streaming options

</div>

<div class="cell code" data-execution_count="5">

``` python
tickerSchema = T.StructType([T.StructField("price", T.StringType(), True),T.StructField("symbol", T.StringType(), True)])
tickerStream = spark.readStream.schema(tickerSchema).option("maxFilePerTrigger", 1).json(LOG_PATH)
```

</div>

<div class="cell markdown">

Apply transformation and create the stream

</div>

<div class="cell code" data-execution_count="6">

``` python
tickerStream = tickerStream.filter(tickerStream.symbol.isin(["BTCUSDT", "DOGEUSDT", "ICPUSDT", "AXSUSDT", "SOLUSDT"]))
print('isStreaming --> ', tickerStream.isStreaming)
streamingQuery = tickerStream.writeStream.queryName("vwTicker").format("memory").outputMode("append")
```

<div class="output stream stdout">

    isStreaming -->  True

</div>

</div>

<div class="cell markdown">

Start streaming from log to the streamed DF

</div>

<div class="cell code" data-execution_count="7">

``` python
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
```

<div class="output stream stdout">

``` 
Count -->  0
+-------+--------------+--------+
| symbol|         price|countAll|
+-------+--------------+--------+
|BTCUSDT|30190.40000000|       4|
|ICPUSDT|    8.43000000|       4|
+-------+--------------+--------+

Count -->  2
+-------+--------------+--------+
| symbol|         price|countAll|
+-------+--------------+--------+
|BTCUSDT|30190.41000000|       7|
|ICPUSDT|    8.44000000|       7|
+-------+--------------+--------+

Count -->  2
+-------+--------------+--------+
| symbol|         price|countAll|
+-------+--------------+--------+
|BTCUSDT|30190.41000000|       9|
|ICPUSDT|    8.44000000|       9|
+-------+--------------+--------+

Count -->  2
+-------+--------------+--------+
| symbol|         price|countAll|
+-------+--------------+--------+
|BTCUSDT|30190.41000000|      10|
|ICPUSDT|    8.44000000|      10|
+-------+--------------+--------+

Count -->  2
+-------+--------------+--------+
| symbol|         price|countAll|
+-------+--------------+--------+
|BTCUSDT|30190.41000000|      11|
|ICPUSDT|    8.44000000|      11|
+-------+--------------+--------+

Count -->  2
+-------+--------------+--------+
| symbol|         price|countAll|
+-------+--------------+--------+
|BTCUSDT|30201.68000000|      12|
|ICPUSDT|    8.44000000|      12|
+-------+--------------+--------+

Count -->  2
+-------+--------------+--------+
| symbol|         price|countAll|
+-------+--------------+--------+
|BTCUSDT|30202.47000000|      14|
|ICPUSDT|    8.44000000|      14|
+-------+--------------+--------+

Count -->  2
+-------+--------------+--------+
| symbol|         price|countAll|
+-------+--------------+--------+
|BTCUSDT|30202.47000000|      15|
|ICPUSDT|    8.44000000|      15|
+-------+--------------+--------+

Count -->  2
+-------+--------------+--------+
| symbol|         price|countAll|
+-------+--------------+--------+
|BTCUSDT|30205.68000000|      17|
|ICPUSDT|    8.44000000|      17|
+-------+--------------+--------+

```

</div>

</div>
