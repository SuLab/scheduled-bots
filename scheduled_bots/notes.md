## Common Issues
### Mongo Cursor Timeout
When iterating through a Mongo cursor (when using the find command), often I'll get a ```pymongo.errors.CursorNotFound: cursor id '...' not valid at server```. More info here: https://stackoverflow.com/a/24200795/1991066
I tried setting the batch_size to 20, but it will still occasionaly timeout. For example, wikidata will randomly slow, take a couple minutes per action, and then reach that 10 min timeout window.
```
 14%|█▍        | 2735/18948 [59:55<11:18:44,  2.51s/it]
 14%|█▍        | 2736/18948 [1:00:04<19:35:01,  4.35s/it]
 14%|█▍        | 2737/18948 [1:00:06<15:56:36,  3.54s/it]
 14%|█▍        | 2738/18948 [1:10:00<814:38:47, 180.92s/it]
 14%|█▍        | 2739/18948 [1:10:05<576:53:56, 128.13s/it]
 14%|█▍        | 2740/18948 [1:10:10<409:32:02, 90.96s/it]
 ```

So, I changed the cursor timeout on mongod to 24 hours by adding the following lines to ```/etc/mongod.conf```:
```
setParameter:
  cursorTimeoutMillis: 86400000
```
and restarting mongod (```sudo service mongod restart```).
More info: https://jira.mongodb.org/browse/SERVER-8188