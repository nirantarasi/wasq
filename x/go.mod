module github.com/nirantarasi/wasq/x

go 1.27.0

require (
	github.com/google/uuid v1.6.0
	github.com/nirantarasi/wasq v0.25.0
	github.com/prometheus/client_golang v1.25.0
	github.com/redis/go-redis/v9 v9.23.0
)

replace github.com/nirantarasi/wasq => ../

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
