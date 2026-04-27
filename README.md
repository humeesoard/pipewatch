# pipewatch

A lightweight CLI tool for monitoring and alerting on data pipeline health metrics in real time.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourusername/pipewatch.git
cd pipewatch
pip install -e .
```

---

## Usage

Start monitoring a pipeline by pointing pipewatch at your metrics endpoint or log source:

```bash
pipewatch monitor --source kafka://localhost:9092 --topic pipeline-metrics
```

Set alert thresholds and get notified when something goes wrong:

```bash
pipewatch monitor --source ./pipeline.log --alert-on error_rate>0.05 --alert-on lag>1000
```

Watch pipeline health in your terminal with a live dashboard:

```bash
pipewatch watch --interval 5 --format table
```

Run `pipewatch --help` to see all available commands and options.

---

## Key Features

- Real-time metric streaming from logs, Kafka, and HTTP endpoints
- Configurable alerting thresholds with email and webhook support
- Lightweight terminal dashboard with live updates
- Simple YAML-based configuration

---

## Configuration

```yaml
# pipewatch.yaml
source: kafka://localhost:9092
topic: pipeline-metrics
interval: 10
alerts:
  - metric: error_rate
    threshold: "> 0.05"
  - metric: lag
    threshold: "> 1000"
```

```bash
pipewatch monitor --config pipewatch.yaml
```

---

## License

This project is licensed under the [MIT License](LICENSE).