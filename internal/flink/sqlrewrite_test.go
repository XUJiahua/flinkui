package flink

import (
	"strings"
	"testing"
)

const kafkaSourceSQL = `CREATE TEMPORARY TABLE evofinder_orders (
  ` + "`id`" + ` VARCHAR,
  WATERMARK FOR ` + "`creationTime`" + ` AS creationTime - INTERVAL '10' SECONDS
)
COMMENT ''
WITH (
  'connector' = 'kafka-safe',
  'format' = 'json',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = '${KAFKA_GROUP_ID}-intpay',
  'scan.startup.mode' = 'earliest-offset',
  'scan.rate-limit.records-per-second' = '100',
  'topic' = '${KAFKA_TOPIC}'
);
-- sink table
CREATE TEMPORARY TABLE dest_redis (
  ` + "`k`" + ` VARCHAR
)
WITH (
  'connector' = 'redis',
  'host' = '${REDIS_HOST}'
);
`

func TestRewriteForReplaySingleSource(t *testing.T) {
	out, table, err := rewriteForReplay(kafkaSourceSQL, "", 1730000000000, "earliest")
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if table != "evofinder_orders" {
		t.Fatalf("chosen table = %q, want evofinder_orders", table)
	}
	// The three keys are upserted...
	for _, want := range []string{
		"'scan.startup.mode' = 'timestamp'",
		"'scan.startup.timestamp-millis' = '1730000000000'",
		"'properties.auto.offset.reset' = 'earliest'",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n---\n%s", want, out)
		}
	}
	// ...the old earliest-offset value is gone...
	if strings.Contains(out, "'earliest-offset'") {
		t.Errorf("stale scan.startup.mode value still present:\n%s", out)
	}
	// ...unrelated options are preserved...
	for _, keep := range []string{
		"'connector' = 'kafka-safe'",
		"'scan.rate-limit.records-per-second' = '100'",
		"'topic' = '${KAFKA_TOPIC}'",
	} {
		if !strings.Contains(out, keep) {
			t.Errorf("output dropped preserved option %q\n%s", keep, out)
		}
	}
	// ...and the redis sink WITH block is untouched.
	if !strings.Contains(out, "'connector' = 'redis'") || !strings.Contains(out, "'host' = '${REDIS_HOST}'") {
		t.Errorf("redis sink block was altered:\n%s", out)
	}
	if strings.Count(out, "timestamp-millis") != 1 {
		t.Errorf("timestamp-millis should appear exactly once:\n%s", out)
	}
}

func TestRewriteForReplayAmbiguousRequiresTable(t *testing.T) {
	two := kafkaSourceSQL + `
CREATE TABLE other_kafka (
  ` + "`x`" + ` VARCHAR
)
WITH (
  'connector' = 'kafka',
  'topic' = 't2'
);
`
	if _, _, err := rewriteForReplay(two, "", 1, "earliest"); err == nil {
		t.Fatal("expected ambiguity error for multiple Kafka sources")
	}
	// Explicit table disambiguates.
	out, table, err := rewriteForReplay(two, "other_kafka", 42, "earliest")
	if err != nil {
		t.Fatalf("rewrite with explicit table: %v", err)
	}
	if table != "other_kafka" {
		t.Fatalf("table = %q, want other_kafka", table)
	}
	// The evofinder_orders block must remain earliest-offset (untouched).
	if !strings.Contains(out, "'scan.startup.mode' = 'earliest-offset'") {
		t.Errorf("non-target Kafka source was modified:\n%s", out)
	}
}

func TestRewriteForReplayNoKafkaSource(t *testing.T) {
	sql := `CREATE TABLE only_redis (` + "`k`" + ` VARCHAR) WITH ('connector' = 'redis');`
	if _, _, err := rewriteForReplay(sql, "", 1, "earliest"); err == nil {
		t.Fatal("expected error when no Kafka source present")
	}
}

func TestRewriteIgnoresCommentedOption(t *testing.T) {
	sql := `CREATE TABLE k (` + "`x`" + ` VARCHAR)
WITH (
  'connector' = 'kafka',
  -- 'scan.startup.mode' = 'group-offsets',
  'topic' = 't'
);`
	out, _, err := rewriteForReplay(sql, "", 99, "earliest")
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	// Exactly one active scan.startup.mode, set to timestamp.
	if strings.Count(stripComments(out), "'scan.startup.mode'") != 1 {
		t.Errorf("expected exactly one active scan.startup.mode:\n%s", out)
	}
	if !strings.Contains(out, "'scan.startup.mode' = 'timestamp'") {
		t.Errorf("scan.startup.mode not set to timestamp:\n%s", out)
	}
}
