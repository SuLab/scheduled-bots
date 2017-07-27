
### to access mysql on wmflabs
```bash
ssh tools-login.wmflabs.org
mysql -h wikidatawiki.labsdb
```
in mysql:
```mysql
use wikidatawiki_p;
select rev_id FROM revision WHERE
  rev_timestamp > DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 5 DAY),'%Y%m%d%H%i%s')
  AND rev_user = 282770;
```

### get revisions by their ids
up to 500 at a time!
https://www.wikidata.org/w/api.php?action=query&prop=revisions&revids=505607374|123|414378313&rvprop=content|ids%7Ctimestamp%7Cflags%7Ccomment%7Cuser&format=json

get the revisions in the last year on one item
https://www.wikidata.org/w/api.php?action=query&prop=revisions&titles=Q10874&rvend=2016-07-01T00%3A00%3A00Z&rvprop=timestamp%7Cuser%7Ccomment&rvdir=older&rvstart=2017-07-01T00%3A00%3A00Z&format=json&rvlimit=50&&rvprop=content|ids%7Ctimestamp%7Cflags%7Ccomment%7Cuser

(a way to speed this up might be to use the mysql db to get the revisions we want, then use the frist query)

