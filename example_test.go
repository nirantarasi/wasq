// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package wasq_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nirantarasi/wasq"
	"golang.org/x/sys/unix"
)

func ExampleServer_Run() {
	srv := wasq.NewServer(
		wasq.RedisClientOpt{Addr: ":6379"},
		wasq.Config{Concurrency: 20},
	)

	h := wasq.NewServeMux()
	// ... Register handlers

	// Run blocks and waits for os signal to terminate the program.
	if err := srv.Run(h); err != nil {
		log.Fatal(err)
	}
}

func ExampleServer_Shutdown() {
	srv := wasq.NewServer(
		wasq.RedisClientOpt{Addr: ":6379"},
		wasq.Config{Concurrency: 20},
	)

	h := wasq.NewServeMux()
	// ... Register handlers

	if err := srv.Start(h); err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGTERM, unix.SIGINT)
	<-sigs // wait for termination signal

	srv.Shutdown()
}

func ExampleServer_Stop() {
	srv := wasq.NewServer(
		wasq.RedisClientOpt{Addr: ":6379"},
		wasq.Config{Concurrency: 20},
	)

	h := wasq.NewServeMux()
	// ... Register handlers

	if err := srv.Start(h); err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGTERM, unix.SIGINT, unix.SIGTSTP)
	// Handle SIGTERM, SIGINT to exit the program.
	// Handle SIGTSTP to stop processing new tasks.
	for {
		s := <-sigs
		if s == unix.SIGTSTP {
			srv.Stop() // stop processing new tasks
			continue
		}
		break // received SIGTERM or SIGINT signal
	}

	srv.Shutdown()
}

func ExampleScheduler() {
	scheduler := wasq.NewScheduler(
		wasq.RedisClientOpt{Addr: ":6379"},
		&wasq.SchedulerOpts{Location: time.Local},
	)

	if _, err := scheduler.Register("* * * * *", wasq.NewTask("task1", nil)); err != nil {
		log.Fatal(err)
	}
	if _, err := scheduler.Register("@every 30s", wasq.NewTask("task2", nil)); err != nil {
		log.Fatal(err)
	}

	// Run blocks and waits for os signal to terminate the program.
	if err := scheduler.Run(); err != nil {
		log.Fatal(err)
	}
}

func ExampleParseRedisURI() {
	rconn, err := wasq.ParseRedisURI("redis://localhost:6379/10")
	if err != nil {
		log.Fatal(err)
	}
	r, ok := rconn.(wasq.RedisClientOpt)
	if !ok {
		log.Fatal("unexpected type")
	}
	fmt.Println(r.Addr)
	fmt.Println(r.DB)
	// Output:
	// localhost:6379
	// 10
}

func ExampleResultWriter() {
	// ResultWriter is only accessible in Handler.
	h := func(ctx context.Context, task *wasq.Task) error {
		// .. do task processing work

		res := []byte("task result data")
		n, err := task.ResultWriter().Write(res) // implements io.Writer
		if err != nil {
			return fmt.Errorf("failed to write task result: %w", err)
		}
		log.Printf(" %d bytes written", n)
		return nil
	}

	_ = h
}
