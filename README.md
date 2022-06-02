
Work in progress.

A job queue that is based on beanstalkd but with some important differences:

- client uses 2 communication channels. One is for issuing commands, and one purely for receiving jobs
- opencensus interface for obtaining metrics
