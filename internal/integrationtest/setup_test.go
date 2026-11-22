package integrationtest

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/nirantarasi/wasq"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestMain(m *testing.M) {
	// Auto-configure Docker socket for Colima/Docker Desktop on macOS if not explicitly set
	if os.Getenv("DOCKER_HOST") == "" {
		homeDir, err := os.UserHomeDir()
		if err == nil {
			colimaSocket := filepath.Join(homeDir, ".colima", "default", "docker.sock")
			if _, err := os.Stat(colimaSocket); err == nil {
				os.Setenv("DOCKER_HOST", "unix://"+colimaSocket)
				if os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE") == "" {
					os.Setenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")
				}
			}
		}
	}
	os.Exit(m.Run())
}

// setupRedisContainer starts an isolated Redis container and returns wasq.RedisConnOpt and a teardown func.
func setupRedisContainer(t *testing.T) (wasq.RedisConnOpt, func()) {
	t.Helper()
	ctx := context.Background()

	redisContainer, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}

	endpoint, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		_ = redisContainer.Terminate(ctx)
		t.Fatalf("failed to get redis connection string: %v", err)
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		_ = redisContainer.Terminate(ctx)
		t.Fatalf("failed to parse redis connection string %q: %v", endpoint, err)
	}

	opt := wasq.RedisClientOpt{
		Addr: u.Host,
	}

	cleanup := func() {
		if err := redisContainer.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate redis container: %v", err)
		}
	}

	return opt, cleanup
}

// setupPostgresContainer starts an isolated PostgreSQL container and returns an open *sql.DB and a teardown func.
func setupPostgresContainer(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wasq_test"),
		tcpostgres.WithUsername("wasq"),
		tcpostgres.WithPassword("wasq_pass"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgContainer.Terminate(ctx)
		t.Fatalf("failed to get postgres connection string: %v", err)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		_ = pgContainer.Terminate(ctx)
		t.Fatalf("failed to open postgres database: %v", err)
	}

	// Verify connection
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		_ = pgContainer.Terminate(ctx)
		t.Fatalf("failed to ping postgres database: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		if err := pgContainer.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}

	return db, cleanup
}

func parseHostPort(addr string) (string, int) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return addr, 6379
	}
	port, _ := strconv.Atoi(parts[1])
	return parts[0], port
}

func mustMarshalJSON(v any) []byte {
	switch val := v.(type) {
	case string:
		return []byte(val)
	case []byte:
		return val
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
}
