package main

import "testing"

func TestNormalizeExpectedSHA256(t *testing.T) {
	got, err := normalizeExpectedSHA256("EEA58D784CD027518789CF18CB809F7A9328F481AC57C00D48778F625A040CA6")
	if err != nil {
		t.Fatalf("normalizeExpectedSHA256: %v", err)
	}
	if got != "eea58d784cd027518789cf18cb809f7a9328f481ac57c00d48778f625a040ca6" {
		t.Fatalf("unexpected normalized sha: %s", got)
	}

	if _, err := normalizeExpectedSHA256("bad"); err == nil {
		t.Fatalf("expected invalid sha error")
	}
}

func TestResolveEmbeddedManifestExpectation(t *testing.T) {
	profile, sha, err := resolveEmbeddedManifestExpectation("", "")
	if err != nil {
		t.Fatalf("resolveEmbeddedManifestExpectation: %v", err)
	}
	if profile == "" {
		t.Fatalf("expected non-empty profile")
	}
	if sha != embeddedDefaultSQLiteSHA256() {
		t.Fatalf("expected sha %s, got %s", embeddedDefaultSQLiteSHA256(), sha)
	}
}

func TestResolveEmbeddedManifestExpectationConflict(t *testing.T) {
	if _, _, err := resolveEmbeddedManifestExpectation("embedded-hsa-extended", ""); err == nil {
		t.Fatalf("expected profile conflict error")
	}
}
