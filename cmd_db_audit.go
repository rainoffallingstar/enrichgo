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
	dbPath                 string
	format                 string
	profile                string
	strictContract         bool
	expectSHA256           string
	expectEmbeddedManifest bool
}

type dbHashCheck struct {
	DBSHA256       string
	ExpectedSHA256 string
	Match          *bool
}

type dbAuditJSONReport struct {
	*store.AuditReport
	DBPath         string `json:"db_path"`
	DBSHA256       string `json:"db_sha256,omitempty"`
	ExpectedSHA256 string `json:"expected_sha256,omitempty"`
	SHA256Match    *bool  `json:"sha256_match,omitempty"`
	Status         string `json:"status"`
}

func runDBAudit(cmd *flag.FlagSet) {
	c := &dbAuditCmd{}

	cmd.StringVar(&c.dbPath, "db", "", "SQLite DB path (required)")
	cmd.StringVar(&c.format, "fmt", "text", "Output format: text, json")
	cmd.StringVar(&c.profile, "profile", "", "Optional data-contract profile: embedded-hsa-seed, embedded-hsa-basic, embedded-hsa-extended-sru (legacy alias: embedded-hsa-extended)")
	cmd.BoolVar(&c.strictContract, "strict-contract", true, "When --profile is set, fail command if contract checks fail")
	cmd.StringVar(&c.expectSHA256, "expect-sha256", "", "Optional expected SHA256 of --db; mismatch fails audit")
	cmd.BoolVar(&c.expectEmbeddedManifest, "expect-embedded-manifest", false, "Use embedded manifest SHA256 + contract_profile as expected values")
	cmd.Parse(os.Args[2:])

	if strings.TrimSpace(c.dbPath) == "" {
		fmt.Fprintln(os.Stderr, "Error: --db is required")
		cmd.Usage()
		os.Exit(1)
	}

	expectedSHA, err := normalizeExpectedSHA256(c.expectSHA256)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	profile := strings.TrimSpace(c.profile)
	if c.expectEmbeddedManifest {
		profile, expectedSHA, err = resolveEmbeddedManifestExpectation(profile, expectedSHA)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}

	st, err := store.OpenSQLite(c.dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening sqlite db: %v\n", err)
		os.Exit(2)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	report, err := st.AuditWithContract(ctx, profile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error auditing sqlite db: %v\n", err)
		os.Exit(2)
	}

	dbSHA, err := fileSHA256(c.dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error hashing sqlite db: %v\n", err)
		os.Exit(2)
	}
	hashCheck := &dbHashCheck{DBSHA256: dbSHA, ExpectedSHA256: expectedSHA}
	if expectedSHA != "" {
		match := strings.EqualFold(dbSHA, expectedSHA)
		hashCheck.Match = &match
	}

	statusOK := report.CoreHealthy()
	if c.strictContract {
		statusOK = report.Healthy()
	}
	if hashCheck.Match != nil && !*hashCheck.Match {
		statusOK = false
	}

	switch strings.ToLower(strings.TrimSpace(c.format)) {
	case "", "text":
		printDBAuditText(c.dbPath, report, hashCheck, statusOK)
	case "json":
		status := "fail"
		if statusOK {
			status = "ok"
		}
		payload, err := json.MarshalIndent(
			dbAuditJSONReport{
				AuditReport:    report,
				DBPath:         c.dbPath,
				DBSHA256:       hashCheck.DBSHA256,
				ExpectedSHA256: hashCheck.ExpectedSHA256,
				SHA256Match:    hashCheck.Match,
				Status:         status,
			},
			"",
			"  ",
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error rendering audit JSON: %v\n", err)
			os.Exit(2)
		}
		fmt.Println(string(payload))
	default:
		fmt.Fprintf(os.Stderr, "Error: unsupported --fmt %q (use text or json)\n", c.format)
		os.Exit(1)
	}

	if !statusOK {
		os.Exit(2)
	}
}

func normalizeExpectedSHA256(raw string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return "", nil
	}
	if !sha256HexPattern.MatchString(v) {
		return "", fmt.Errorf("invalid --expect-sha256 value %q", raw)
	}
	return v, nil
}

func resolveEmbeddedManifestExpectation(profile, expectedSHA string) (string, string, error) {
	manifest, err := embeddedDefaultSQLiteManifest()
	if err != nil {
		return "", "", fmt.Errorf("load embedded manifest: %w", err)
	}

	profile = strings.TrimSpace(profile)
	if profile == "" {
		profile = manifest.ContractProfile
	} else if !strings.EqualFold(profile, manifest.ContractProfile) {
		return "", "", fmt.Errorf(
			"--profile %q conflicts with embedded manifest contract_profile %q",
			profile,
			manifest.ContractProfile,
		)
	}

	expectedSHA = strings.ToLower(strings.TrimSpace(expectedSHA))
	if expectedSHA == "" {
		expectedSHA = manifest.SHA256
	} else if expectedSHA != manifest.SHA256 {
		return "", "", fmt.Errorf(
			"--expect-sha256 %q conflicts with embedded manifest sha256 %q",
			expectedSHA,
			manifest.SHA256,
		)
	}

	return profile, expectedSHA, nil
}

func printDBAuditText(dbPath string, report *store.AuditReport, hashCheck *dbHashCheck, statusOK bool) {
	fmt.Printf("db=%s\n", dbPath)
	fmt.Printf("has_schema_version=%t\n", report.HasSchemaVersion)
	fmt.Printf("schema_version=%d\n", report.SchemaVersion)
	fmt.Printf("current_schema_version=%d\n", report.CurrentSchemaVersion)
	fmt.Printf("tables_valid=%t\n", report.TablesValid)
	fmt.Printf("indexes_valid=%t\n", report.IndexesValid)
	if strings.TrimSpace(report.ValidationError) != "" {
		fmt.Printf("validation_error=%s\n", report.ValidationError)
	}
	for _, table := range []string{"meta", "dataset", "term", "gene_dict", "term_gene", "idmap_canon", "geneset", "geneset_gene", "idmap"} {
		if n, ok := report.RowCounts[table]; ok {
			fmt.Printf("rows.%s=%d\n", table, n)
		}
	}
	if strings.TrimSpace(report.ContractProfile) != "" {
		fmt.Printf("contract_profile=%s\n", report.ContractProfile)
		fmt.Printf("contract_valid=%t\n", report.ContractValid)
		for _, v := range report.ContractViolations {
			fmt.Printf("contract_violation=%s\n", v)
		}
	}
	if hashCheck != nil {
		fmt.Printf("db_sha256=%s\n", hashCheck.DBSHA256)
		if hashCheck.ExpectedSHA256 != "" {
			fmt.Printf("expected_sha256=%s\n", hashCheck.ExpectedSHA256)
			if hashCheck.Match != nil {
				fmt.Printf("sha256_match=%t\n", *hashCheck.Match)
			}
		}
	}
	if statusOK {
		fmt.Println("status=ok")
	} else {
		fmt.Println("status=fail")
	}
}
