# Scheduler (Go)

Package scheduler implements a scheduler for rate limited operations
using a prioritized queue.

## Use Case

This package is built to schedule operations against rate limited API's.
More specifically it's meant for applications which need to perform both
real-time operations as well as a hefty amount of background scraping.

## Documentation

See [godoc](https://godoc.org/github.com/boljen/go-scheduler) for more information.

## License

Released under the MIT license.
