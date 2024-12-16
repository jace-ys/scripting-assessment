package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var (
	otlpMetricsEndpoint = kingpin.Flag("otlp-metrics-endpoint", "OTLP HTTP endpoint for exporting metrics to.").Envar("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").Default("http://localhost:9090/api/v1/otlp/v1/metrics").String()
	otlpExportInterval  = kingpin.Flag("export-interval", "Interval for pushing metrics").Envar("OTEL_EXPORTER_OTLP_METRICS_INTERVAL").Default("5s").Duration()
	ethNodeURL          = kingpin.Flag("eth-node-url", "URL of the ETH Node API to collect metrics from.").Envar("ETH_NODE_URL").Default("https://ethereum.publicnode.com").URL()
)

func main() {
	kingpin.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()

	if err := run(ctx); err != nil {
		slog.Error("exporter failed to run", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName("eth-node"), semconv.ServiceVersion("1.0.0")),
	)
	if err != nil {
		return fmt.Errorf("metric resource: %w", err)
	}

	exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(*otlpMetricsEndpoint))
	if err != nil {
		return fmt.Errorf("init exporter: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(5*time.Second))),
	)

	otel.SetMeterProvider(provider)
	defer func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			log.Printf("error shutting down meter provider: %s", err)
		}
	}()

	lastBlockNumberGauge, err := otel.Meter("github.com/jace-ys/scripting-assessment/your-probe").Int64Gauge(
		"eth_node.last_block_number",
		metric.WithDescription("Integer of the current block number the ETH Node is on."),
	)
	if err != nil {
		return fmt.Errorf("init metric eth_node.last_block_number: %s", err)
	}

	coll := NewETHNodeCollector(*ethNodeURL)

	ticker := time.NewTicker(*otlpExportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down metric exporter")
			return nil
		case <-ticker.C:
			slog.Info("collecting last block number from ETH node")
			num, err := coll.getLastBlock()
			if err != nil {
				slog.Error("failed to collect last block number from ETH node", slog.String("err", err.Error()))
			} else {
				slog.Info("exporting last block number from ETH node", slog.Int64("number", num))
				lastBlockNumberGauge.Record(ctx, num)
			}
		}
	}
}

type ETHNodeCollector struct {
	apiURL *url.URL
	client *http.Client
}

func NewETHNodeCollector(apiURL *url.URL) *ETHNodeCollector {
	return &ETHNodeCollector{
		apiURL: apiURL,
		client: &http.Client{
			Timeout: time.Second,
		},
	}
}

type NodeBody struct {
	Result NodeBodyResult `json:"result"`
}

type NodeBodyResult struct {
	Number string `json:"number"`
}

func (c *ETHNodeCollector) getLastBlock() (int64, error) {
	reqBody, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params":  []any{"latest", false},
		"id":      1,
	})
	if err != nil {
		return 0, fmt.Errorf("marshal request body: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, c.apiURL.String(), bytes.NewReader(reqBody))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	rsp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("do request: %w", err)
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != 200 {
		return 0, fmt.Errorf("unexpected response status code: %d", rsp.StatusCode)
	}

	var nodeBody NodeBody
	if err := json.NewDecoder(rsp.Body).Decode(&nodeBody); err != nil {
		return 0, fmt.Errorf("unmarshal response body: %s", err)
	}

	num, err := strconv.ParseInt(nodeBody.Result.Number, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("parse last block number: %s", err)
	}

	return num, nil
}
