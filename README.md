
Work in progress.

A job queue that is based on beanstalkd but with some important differences:

- session can consist of one or more connections. One is for issuing commands, and one purely for receiving jobs
- opencensus interface for obtaining metrics
- jobcannon still allows reserves when deadline of an earlier reserved job is nearby. It does not send DeadlineNearby.
