The function starts from the `main.py` where the Neo4j initialization is done.
SPARQL queries are fired after that for `events` and `eventseries`. After that we use the `pylookupParser` to extract
the `ordinality` of the `events`

Start with the matching process:

* Remove the previous matched events and event series
* Preprocessing to extract the `titles` from the `events` and `eventseries`
* Call methods for full matches using title
* Call the NLP matcher for partial matches

