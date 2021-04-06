# CurrencyEchangeOrderMatchingS3

Cuurency Exchange 

Currency exchange platform. Takes
Takes in CurrencyExchange orders
stores them on on a storage platform. 
Stores them as currency exchange orders in the cache 
Matches currency exchange orders in the cache

Removes  all matched from cache or expired from the the cache and store on a storage platform. 


The orders will come in as a continues stream via messenger. Speed of processing is essential 


Each message when it comes in spawns a  process. 
The process will then spawn 3 processes to store, load and match orders. 
Remove orders will be run as a schedule

Within the process, new processes are spawned Such as in matching.
To match all buys with all sells, Each buy search will spawn its only process that will match a buy with a sell,
This can cause problems such as race conditions. So the code makes use of locking and CopyOnWriteArrayList.
