# Docker Image

Docker images can be found at [https://hub.docker.com/repository/docker/jsoneventsourcing/pincette-json-streams](https://hub.docker.com/repository/docker/jsoneventsourcing/pincette-json-streams). You should add a configuration layer with a Docker file that looks like this:

```
FROM registry.hub.docker.com/jsoneventsourcing/pincette-json-streams:<version>
COPY conf/tst.conf /conf/application.conf
```

So, wherever your configuration file comes from, it should always end up at `/conf/application.conf`.

If you add plugins then they should be added to the image. Say you set `plugins` to "/plugins" in the configuration. The Docker file should then have a line like this:

```
ADD plugins/* /plugins
```
