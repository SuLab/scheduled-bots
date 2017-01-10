# scheduled-bots

These bots are running on [Jenkins](http://34.193.174.196:8080/), which is hosted on AWS.



## Bot Creation Guidelines

Insert Bot Creation Guidelines here

### Data Sources
Data used by a bot, that is regularly updated by an external source, should be handled by [our instance](https://github.com/stuppie/wdbiothings) of the [Biothings.api](https://github.com/SuLab/biothings.api). The bot should access the data from the mongodb server which is running on the same instance as Jenkins.

> mongo 34.193.174.196/wikidata_src -u sulab -p PASS


## Bots
Name | Data Source | Jenkins
--- | --- | ---
GeneBot | [mygene.info](https://github.com/stuppie/wdbiothings/tree/master/wdbiothings/contrib/mygene) | [human](http://34.193.174.196:8080/job/GeneBot_Homo_sapiens/), [yeast](http://34.193.174.196:8080/job/GeneBot_yeast/), [microbes](http://34.193.174.196:8080/job/GeneBot_microbes/)
ProteinBot | [mygene.info](https://github.com/stuppie/wdbiothings/tree/master/wdbiothings/contrib/mygene) | [human](http://34.193.174.196:8080/job/ProteinBot_homo_sapiens/), [yeast](http://34.193.174.196:8080/job/ProteinBot_yeast/), [microbes](TODO)
GoAnnotationBot | [quickgo](https://github.com/stuppie/wdbiothings/tree/master/wdbiothings/contrib/quickgo) | [currently yeast, todo set to everything](http://34.193.174.196:8080/job/GOBot/)
GoAnnotationBot | [quickgo](https://github.com/stuppie/wdbiothings/tree/master/wdbiothings/contrib/quickgo) | [human (bigmem)](http://34.193.174.196:8080/job/GOBot_bigmem/)
InterproBot | [interpro](https://github.com/stuppie/wdbiothings/tree/master/wdbiothings/contrib/interpro) | [all](http://34.193.174.196:8080/job/interpro/)
