package main

import (
	"context"
	"flag"
	"github.com/turbosquid/ticketd/http"
	"github.com/turbosquid/ticketd/ticket"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const VERSION = "0.0.1"

func main() {
	log.Printf("TicketD v%s starts...", VERSION)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// Flags
	listenOn := flag.String("l", "0.0.0.0:8001", "Address/port to listen on")
	snapshotPath := flag.String("snappath", "", "Snapshot path")
	expireInterval := flag.Int("expire", 500, "Expiration interval in ms")
	snapshotInterval := flag.Int("snapshot", 1000, "Snapshot interval in ms")
	logLevel := flag.Int("loglevel", 1, "Numeric log level")
	flag.Parse()
	td := ticket.NewTicketD(*expireInterval, *snapshotPath, *snapshotInterval, &ticket.DefaultLogger{*logLevel})
	td.Start()
	svr := http.StartServer(*listenOn, td)
	sig := <-sigs
	log.Printf("Received signal %#v", sig)
	svr.Shutdown(context.Background())
	td.Quit()
	log.Printf("Done.")
}
