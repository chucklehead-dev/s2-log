# S2 Log for XTDB

A [Log](https://docs.xtdb.com/ops/config/log.html) implementation for [XTDB](https://xtdb.com) using [S2](https://s2.dev).

## Current Status

Integration tests based on [Kafka](https://github.com/xtdb/xtdb/blob/main/modules/kafka/src/test/kotlin/xtdb/api/log/KafkaLogTest.kt) module are passing, but encountering packaging/dependency issues when trying to test against [xtdb-bench](https://github.com/xtdb/xtdb/tree/main/modules/bench).

## Setup

### S2

* Create an account at https://s2.dev
* Review the [quickstart](https://s2.dev/docs/quickstart) to setup the CLI, or use the [dashboard](https://s2.dev/dashboard)
* Create a basin and stream
  * Recommended settings for stream (to be refined):
    * Storage class: your choice, will provide benchmarks when possible
    * Retention Age: Between 1-7 days (86400-604800 seconds) based on recommended retention settings for [Kafka](https://docs.xtdb.com/ops/config/log/kafka.html#_setup) log
    * Timestamping Mode: `arrival`
* Create an [access token](https://s2.dev/docs/access-control) with permissions to read and write to the stream

### Github

See [here](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry#authenticating-to-github-packages) for how to create a personal access token for accessing Github Maven repositories.

### Dependency

#### Clojure
```clojure
{:deps {dev.chucklehead/s2-log {:mvn/version "0.0.4"}}
 :mvn/repos {"chucklehead" {:url "https://maven.pkg.github.com/chucklehead-dev/s2-log"}}}
```

## Configuration

Configuration is supported using XTDB's YAML configuration
```yaml
log: !S2
  token: !Env S2_ACCESS_TOKEN
  basin: !Env S2_BASIN
  stream: !Env S2_STREAM
```

Or via the Clojure API. 

Note that `::cxt/s2-log` replaces the entire `:log` element from the [Clojure Configuration Cookbook](https://docs.xtdb.com/ops/config/clojure.html), rather than being a child element like `:local`,`:in-memory`, or`:kafka`.

```clojure
(comment
  (require '[chucklehead.xtdb :as cxt])
  (require '[xtdb.node :as xtn])
  
  (xtn/start-node {::cxt/s2-log {:token (System/getenv "S2_ACCESS_TOKEN")
                                 :basin (System/getenv "S2_BASIN")
                                 :stream (System/getenv "S2_STREAM")}})
```