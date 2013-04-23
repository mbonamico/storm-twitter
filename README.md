storm-twitter
=============

Simple example of using Storm to process real-time tweets and store it in mongoDB.

##  Storm

Our main purpose was to use [storm](http://storm-project.net/) for real-time processing and summarization of tweets.
We use [twitter4j](http://twitter4j.org/en/index.html) to access the stream with a username, password and a query to filter.
For every tweet that match the filter we extract the words related to it and make a simple incremental word count.
Each summarization is stored in [mongoDB](http://www.mongodb.org/) using the java-driver provided.
For more information reference the [storm concepts wiki](https://github.com/nathanmarz/storm/wiki/Concepts). Also we suggest to test the 
[storm-starter] (https://github.com/nathanmarz/storm-starter) project to get a better idea of storm.


# Dependencies
    $ install and configure storm. Guidelines here -> https://github.com/nathanmarz/storm/wiki/Setting-up-development-environment
 
# Configuration:
    $ git clone proj.git
    $ cd storm-twitter
    $ edit twitter-properties file located in src/main/resource to configure the twitter account and the query to filter

# Build

    $ mvn compile
    $ mvn package

# Run

    $ storm jar target/storm-twitter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.TwitterStorm

 
