Misc improvements to Apache Lucene & Solr - currently only Solr
* New features
* Performance improvements
* etc

lucene/ is a search engine library
solr/ is a search engine server that uses lucene

To compile the sources run 'ant compile'
To run all the tests run 'ant test'
To setup your IDE run 'ant idea' or 'ant eclipse'
For Maven info, see dev-tools/maven/README.maven.

If you also have trouble getting your contributions committed at Apache Solr, feel free to fork and make pull requests

The following branches point to code containing the improvements (the rest of the branches are just replica of Apache branches)
* my_lucene_solr_4_4_0: Release/tag lucene_solr_4_4_0 from Apache plus all current improvements

The following tags point to released code containing the improvements (the rest of the branches are just replica of Apache branches)
* Currently none

List of improvements
* Support for optimistic locking - the cool solution (https://issues.apache.org/jira/browse/SOLR-3173, https://issues.apache.org/jira/browse/SOLR-3178 and https://issues.apache.org/jira/browse/SOLR-3382)
** Typed exceptions (VersionConflict, DocAlredyExists and DocDoesNotExist) propagated to SolrJ client
** Support for separate results/exceptions for each individual document to be added/updated/deleted in multi-document update requests, so that some can fail with (different) optimistic locking exceptions while others succeed
** Easy backward compatibility (to the update semantics that Solr had before 4.0)
** etc.
* Security
** Support for basic http auth in internal solr requests (https://issues.apache.org/jira/browse/SOLR-4470)
** Support for protecting content in ZK (https://issues.apache.org/jira/browse/SOLR-4580)
* Correctly routed realtime-get through CloudSolrServer (https://issues.apache.org/jira/browse/SOLR-5360)
* Slow response on facet search, lots of facets, asking for few facets in response (https://issues.apache.org/jira/browse/SOLR-5444)
* Lots of other improvements and cleanup

NO WARRANTY!!!
