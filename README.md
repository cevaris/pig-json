pig-json
========

JSON parser/loader for Pig &lt; 0.10.0

Sourced from here
[https://github.com/mmay/PigJsonLoader](https://github.com/mmay/PigJsonLoader)

## Quickstart

1. Get the code: `git clone https://github.com/cevaris/pig-json.git`
1. Build the jar: `mvn package`


### Load JSON to Map
```
a = LOAD 'tweets.json' using org.apache.pig.udfs.json.JsonLoader() as (json:map[]);
b = foreach a generate FLATTEN(json#'entities') as entities;
c = foreach b generate flatten(entities#'urls') as urls;
d = foreach c generate flatten(urls#'url') as url;
DUMP d;
```


### Parse JSON String to Map
```
a = LOAD 'tweets.json' AS (text:chararray);
b = foreach a generate FLATTEN(org.apache.pig.udfs.json.JsonToMap(text)) as json;
c = foreach b generate FLATTEN(json#'entities') as entities;
d = foreach c generate flatten(entities#'urls') as urls;
e = foreach d generate flatten(urls#'url') as url;
DUMP e;
```

## License

Apache licensed.