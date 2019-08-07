 ## Obographs
 
 Obographs is collection of python scripts built on the Wikidata Integrator library, to parse and serialize knowledge available in OWL files to Wikidata. 
 
 Currently the Gene Wiki bots running synchronizing the Disease Ontology and the Ontology rely on this set of python scripts. 
 
 ** This README is work in progress. **
 
 ### Step 1. Creating a JSON model of an OWL file
 #### Disease Ontology
 The Disease Ontology, available as OWL file at http://purl.obolibrary.org/obo/doid.owl is downloaded and parsed in a [designated python script]https://github.com/SuLab/scheduled-bots/blob/master/scheduled_bots/disease_ontology/trigger.py)
 
 1. Downloaded
 2. Stored for pickup by the Disease Ontology both as OBOGraph_DO.py
  