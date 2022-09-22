# ronin-kafka

Project Ronin Kafka library

# Status

Currently a work-in-progress, but usable for non-production systems at this point.

The TODO list:

1. Document usage and example projects
   1. `README.md` usage
   2. Probably need a spring-boot example for both producer and consumer? Currently just have simple kotlin apps
2. Setup GHA workflows for
   1. unit tests
   2. codecov
   3. deployment to nexus
3. Review logging messages and levels
4. `RoninProducer`
   1. Add some exception handling
   2. Metrics
   3. Add support for custom partitioner
5. `RoninConsumer`
   1. Metrics
   2. Add support for consumer re-balance events?
6. Test out access patterns and usability with product engineering
7. Confirm/tweak default Kafka configurations
