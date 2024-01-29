package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp" // TODO: Add all auth providers
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"

	"github.com/lwolf/kube-cleanup-operator/pkg/controller"
)

var (
	gitsha    string
	committed string
)

func setupLogging() {
	// Set logging output to standard console out
	log.SetOutput(os.Stdout)

	// kubernetes client-go uses klog, which logs to file by default. Change defaults to log to stderr instead of file.
	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)
	logtostderr := klogFlags.Lookup("logtostderr")
	logtostderr.Value.Set("true")
}

func main() {
	runOutsideCluster := flag.Bool("run-outside-cluster", false, "Set this flag when running outside of the cluster.")
	namespace := flag.String("namespace", "", "Limit scope to a single namespace")
	listenAddr := flag.String("listen-addr", "0.0.0.0:7000", "Address to expose metrics.")

	deleteSuccessAfter := flag.Duration("delete-successful-after", 15*time.Minute, "Delete jobs in successful state after X duration (golang duration format, e.g 5m), 0 - never delete")
	deleteFailedAfter := flag.Duration("delete-failed-after", 0, "Delete jobs in failed state after X duration (golang duration format, e.g 5m), 0 - never delete")
	ignoreOwnedByCronjob := flag.Bool("ignore-owned-by-cronjobs", false, "[EXPERIMENTAL] Do not cleanup jobs created by cronjobs")

	dryRun := flag.Bool("dry-run", false, "Print only, do not delete anything.")

	labelSelector := flag.String("label-selector", "", "Delete only jobs that meet label selector requirements")

	flag.Parse()
	setupLogging()

	log.Printf("Starting the application. Version: %s, CommitTime: %s\n", gitsha, committed)
	var optsInfo strings.Builder
	optsInfo.WriteString("Provided options: \n")
	optsInfo.WriteString(fmt.Sprintf("\tnamespace: %s\n", *namespace))
	optsInfo.WriteString(fmt.Sprintf("\tdry-run: %v\n", *dryRun))
	optsInfo.WriteString(fmt.Sprintf("\tdelete-successful-after: %s\n", *deleteSuccessAfter))
	optsInfo.WriteString(fmt.Sprintf("\tdelete-failed-after: %s\n", *deleteFailedAfter))
	optsInfo.WriteString(fmt.Sprintf("\tignore-owned-by-cronjobs: %v\n", *ignoreOwnedByCronjob))

	optsInfo.WriteString(fmt.Sprintf("\tlabel-selector: %s\n", *labelSelector))
	log.Println(optsInfo.String())

	sigsCh := make(chan os.Signal, 1) // Create channel to receive OS signals
	stopCh := make(chan struct{})     // Create channel to receive stopCh signal

	signal.Notify(sigsCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT) // Register the sigsCh channel to receieve SIGTERM

	wg := &sync.WaitGroup{}

	// Create clientset for interacting with the kubernetes cluster
	clientset, err := newClientSet(*runOutsideCluster)
	if err != nil {
		log.Fatal(err.Error())
	}
	ctx := context.Background()

	wg.Add(1)
	go func() {
		controller.NewKleaner(
			ctx,
			clientset,
			*namespace,
			*dryRun,
			*deleteSuccessAfter,
			*deleteFailedAfter,
			*ignoreOwnedByCronjob,
			*labelSelector,
			stopCh,
		).Run()
		wg.Done()
	}()
	log.Printf("Controller started...")

	server := http.Server{Addr: *listenAddr}
	wg.Add(1)
	go func() {
		// Expose the registered metrics at `/metrics` path.
		http.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
			metrics.WritePrometheus(w, true)
		})
		err := server.ListenAndServe()
		if err != nil {
			log.Fatalf("failed to ListenAndServe metrics server: %v\n", err)
		}
		wg.Done()
	}()
	log.Printf("Listening at %s", *listenAddr)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			<-stopCh
			log.Println("shutting http server down")
			err := server.Shutdown(ctx)
			if err != nil {
				log.Printf("failed to shutdown metrics server: %v\n", err)
			}
			break
		}
	}()

	<-sigsCh // Wait for signals (this hangs until a signal arrives)
	log.Printf("got termination signal...")
	close(stopCh) // Tell goroutines to stopCh themselves
	wg.Wait()     // Wait for all to be stopped
}

func newClientSet(runOutsideCluster bool) (*kubernetes.Clientset, error) {
	kubeConfigLocation := ""

	if runOutsideCluster {
		if os.Getenv("KUBECONFIG") != "" {
			kubeConfigLocation = filepath.Join(os.Getenv("KUBECONFIG"))
		} else {
			homeDir := os.Getenv("HOME")
			kubeConfigLocation = filepath.Join(homeDir, ".kube", "config")
		}
	}

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfigLocation)

	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}
