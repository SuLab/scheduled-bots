# scheduled-bots

These bots are running on [Jenkins](http://jenkins.sulab.org/), which is hosted on AWS.



## Bot Creation Guidelines


### Data Sources
Data used by a bot, that is regularly updated by an external source, should be handled by [our instance](https://github.com/SuLab/wdbiothings) of the [Biothings.api](https://github.com/SuLab/biothings.api). The bot should access the data from the mongodb server which is running on the same instance as Jenkins.


## Bots
See [Bot Status](https://www.wikidata.org/w/index.php?title=User:ProteinBoxBot/Bot_Status)


## Wikidata - Disease Ontology Feedback Loop

Code for detecting changes and creating robot templates is located here:
https://github.com/SuLab/scheduled-bots/blob/master/scheduled_bots/disease_ontology/robot/run.py
