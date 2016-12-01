# scheduled-bots

These bots are running on [Jenkins](http://34.193.174.196:8080/), which is hosted on AWS.



## Bot Creation Guidelines

Insert Bot Creation Guidelines here

### Data Sources
Data used by a bot, that is regularly updated by an external source, should be handled by [our instance](https://github.com/stuppie/wdbiothings) of the [Biothings.api](https://github.com/SuLab/biothings.api). The bot should access the data from the mongodb server which is running on the same instance as Jenkins.

> mongo 34.193.174.196/wikidata_src -u sulab -p PASS

