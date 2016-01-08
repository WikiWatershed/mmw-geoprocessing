# mmw-geoprocessing

A [Spark Job Server](https://github.com/spark-jobserver/spark-jobserver) job for Model My Watershed geoprocessing.

## Usage

First, build the assembly JAR for this project:

```bash
$ git clone https://github.com/WikiWatershed/mmw-geoprocessing.git
$ cd mmw-geoprocessing
$ ./sbt assembly
```

To use a Docker based Scala build environment, you can use:

```bash
$ docker run \
    --rm \
    --volume ${HOME}/.ivy2:/root/.ivy2 \
    --volume ${PWD}:/mmw-geoprocessing \
    --workdir /mmw-geoprocessing \
    quay.io/azavea/scala:2.10.5 ./sbt assembly
```

Next, use the latest Spark Job Server (SJS) Docker image to launch an instance of SJS locally:

```bash
$ docker run \
    --detach \
    --env AWS_PROFILE=nondefault \
    --volume ${HOME}/.aws:/root/.aws:ro \
    --volume ${PWD}/examples/conf/spark-jobserver.conf:/opt/spark-jobserver/spark-jobserver.conf:ro \
    --publish 8090:8090 \
    --name spark-jobserver \
    quay.io/azavea/spark-jobserver:0.6.1
```

**Note**: Ensure that the `default` credentials in your `$HOME/.aws/credentials` file is set with the appropriate AWS API keys. Otherwise, you may have to set the `AWS_PROFILE` environment variable to your custom credential profile.

Now that the SJS service is running in the background, upload the assembly JAR and create a long-lived Spark context named `geoprocessing`:

```bash
$ curl --silent \
    --data-binary @summary/target/scala-2.10/mmw-geoprocessing-assembly-0.1.0.jar \
    'http://localhost:8090/jars/geoprocessing'
$ curl --silent --data "" \
    'http://localhost:8090/contexts/geoprocessing-context'
```

Once that process is complete, try submitting a job to the `geoprocessing-context`:

```bash
$ curl --silent \
    --data-binary @examples/request.json \
    'http://localhost:8090/jobs?sync=true&context=geoprocessing-context&appName=geoprocessing&classPath=org.wikiwatershed.mmw.geoprocessing.SummaryJob'
```

## Deployments

Deployments to GitHub Releases are handled through [Travis-CI](https://travis-ci.org/WikiWatershed/mmw-geoprocessing). The following `git-flow` commands signal to Travis that we want to create a release:

``` bash
$ git flow release start 0.1.0
$ vim CHANGELOG.md
$ vim project/build.scala
$ git commit -m "0.1.0"
$ git flow release publish 0.1.0
$ git flow release finish 0.1.0
```

You should now check the `develop` and `master` branches on github to make sure that they look correct.  In particular, they should both contain the changes that you made to `CHANGELOG.md`.  If they do not, then the following two steps may also be required:
```bash
$ git push origin develop:develop
$ git push origin master:master
```

To actually kick off the deployment, ensure that the newly created Git tags are pushed remotely with `git push --tags`.
