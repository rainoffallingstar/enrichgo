package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"enrichgo/pkg/store"
)

type dbAuditCmd struct {
	dbPath string
	format string
}

func runDBAudit(cmd *flag.FlagSet) {
	c := &dbAuditCmd{}

	cmd.StringVar(&c.dbPath, "db", "", "SQLite DB path (required)")
	cmd.StringVar(&c.format, "fmt", "text", "Output format: text, json")
	cmd.Parse(os.Args[2:])

	if strings.TrimSpace(c.dbPath) == "" {
		fmt.Fprintln(os.Stderr, "Error: --db is required")
		cmd.Usage()
		os.Exit(1)
	}

	st, err := store.OpenSQLite(c.dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening sqlite db: %v\n", err)
		os.Exit(2)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	report, err := st.Audit(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error auditing sqlite db: %v\n", err)
		os.Exit(2)
	}

	switch strings.ToLower(strings.TrimSpace(c.format)) {
	case "", "text":
		printDBAuditText(c.dbPath, report)
	case "json":
		payload, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error rendering audit JSON: %v\n", err)
			os.Exit(2)
		}
		fmt.Println(string(payload))
	default:
		fmt.Fprintf(os.Stderr, "Error: unsupported --fmt %q (use text or json)\n", c.format)
		os.Exit(1)
	}

	if !report.Healthy() {
		os.Exit(2)
	}
}

func printDBAuditText(dbPath string, report *store.AuditReport) {
	fmt.Printf("db=%s\n", dbPath)
	fmt.Printf("has_schema_version=%t\n", report.HasSchemaVersion)
	fmt.Printf("schema_version=%d\n", report.SchemaVersion)
	fmt.Printf("current_schema_version=%d\n", report.CurrentSchemaVersion)
	fmt.Printf("tables_valid=%t\n", report.TablesValid)
	fmt.Printf("indexes_valid=%t\n", report.IndexesValid)
	if strings.TrimSpace(report.ValidationError) != "" {
		fmt.Printf("validation_error=%s\n", report.ValidationError)
	}
	for _, table := range []string{"meta", "geneset", "geneset_gene", "idmap"} {
		if n, ok := report.RowCounts[table]; ok {
			fmt.Printf("rows.%s=%d\n", table, n)
		}
	}
	if report.Healthy() {
		fmt.Println("status=ok")
	} else {
		fmt.Println("status=fail")
	}
}
