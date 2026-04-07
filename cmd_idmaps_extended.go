package main

import (
	"fmt"
	"os"
	"strings"

	"enrichgo/pkg/annotation"
	"enrichgo/pkg/database"
	"enrichgo/pkg/store"
)

type streamDropStats struct {
	seen    int64
	kept    int64
	dropped int64
}

type extendedIDMapStep struct {
	source   string
	fromType string
	toType   string
	label    string
	stats    *streamDropStats
	produce  func(store.IDMapEmit) error
}

func makeFilteredEmit(emit store.IDMapEmit, stats *streamDropStats) store.IDMapEmit {
	return func(from, to string) error {
		if stats != nil {
			stats.seen++
		}
		from = strings.TrimSpace(from)
		to = strings.TrimSpace(to)
		if from == "" || to == "" {
			if stats != nil {
				stats.dropped++
			}
			return nil
		}
		if stats != nil {
			stats.kept++
		}
		return emit(from, to)
	}
}

func buildExtendedIDMapSteps(species string, taxID int, client database.HTTPClient) []extendedIDMapStep {
	return []extendedIDMapStep{
		{
			source:   "ncbi_gene_info",
			fromType: string(annotation.IDSymbol),
			toType:   string(annotation.IDEntrez),
			produce: func(emit store.IDMapEmit) error {
				return database.StreamNCBIGeneInfoForSpecies(species, taxID, client,
					func(entrez, symbol string) error { return nil },
					func(symbol, entrez string) error { return emit(symbol, entrez) },
				)
			},
		},
	}
}

func logExtendedIDMapStepStats(step extendedIDMapStep) {
	if step.stats == nil || strings.TrimSpace(step.label) == "" {
		return
	}
	fmt.Fprintf(os.Stderr, "Info: idmaps %s->ENTREZ kept=%d dropped_empty=%d seen=%d\n",
		step.label, step.stats.kept, step.stats.dropped, step.stats.seen)
}
