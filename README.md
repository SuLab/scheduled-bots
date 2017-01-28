# scheduled-bots

These bots are running on [Jenkins](http://34.193.174.196:8080/), which is hosted on AWS.



## Bot Creation Guidelines


### Data Sources
Data used by a bot, that is regularly updated by an external source, should be handled by [our instance](https://github.com/stuppie/wdbiothings) of the [Biothings.api](https://github.com/SuLab/biothings.api). The bot should access the data from the mongodb server which is running on the same instance as Jenkins.

> mongo 34.193.174.196/wikidata_src -u sulab -p PASS


## Bots
Name | Data Source | Jenkins
--- | --- | ---
[GeneBot](https://github.com/SuLab/scheduled-bots/blob/master/scheduled_bots/geneprotein/GeneBot.py) | [mygene.info](https://github.com/stuppie/wdbiothings/tree/master/wdbiothings/contrib/mygene) | [human](http://34.193.174.196:8080/job/GeneBot_Homo_sapiens/), [yeast](http://34.193.174.196:8080/job/GeneBot_yeast/), [microbes](http://34.193.174.196:8080/job/GeneBot_microbes/)
[ProteinBot](https://github.com/SuLab/scheduled-bots/blob/master/scheduled_bots/geneprotein/ProteinBot.py) | [mygene.info](https://github.com/stuppie/wdbiothings/tree/master/wdbiothings/contrib/mygene) | [human](http://34.193.174.196:8080/job/ProteinBot_homo_sapiens/), [yeast](http://34.193.174.196:8080/job/ProteinBot_yeast/), [microbes]
[GoAnnotationBot](https://github.com/SuLab/scheduled-bots/blob/master/scheduled_bots/geneprotein/GOBot.py) | [quickgo](https://github.com/stuppie/wdbiothings/tree/master/wdbiothings/contrib/quickgo) | [all](http://34.193.174.196:8080/job/GOBot_bigmem/)
[GOOntologyBot](https://github.com/SuLab/scheduled-bots/blob/master/scheduled_bots/ontology/obo_importer.py) | [OLS](http://www.ebi.ac.uk/ols/api/ontologies/go) | [link](http://34.193.174.196:8080/job/Gene_Ontology/)
[InterproBot](https://github.com/SuLab/scheduled-bots/tree/master/scheduled_bots/interpro) | [interpro](https://github.com/stuppie/wdbiothings/tree/master/wdbiothings/contrib/interpro) | [all](http://34.193.174.196:8080/job/interpro/)
