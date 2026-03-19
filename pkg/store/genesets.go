package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"enrichgo/pkg/types"
)

type GeneSetRow struct {
	ID          string
	Name        string
	Description string
}

type GeneSetFilter struct {
	DB         string
	Species    string
	Ontology   string
	Collection string
}

func normalizeFilter(f GeneSetFilter) GeneSetFilter {
	f.DB = strings.ToLower(strings.TrimSpace(f.DB))
	f.Species = strings.ToLower(strings.TrimSpace(f.Species))
	f.Ontology = strings.ToUpper(strings.TrimSpace(f.Ontology))
	f.Collection = strings.ToLower(strings.TrimSpace(f.Collection))
	if f.Ontology == "" {
		f.Ontology = "-"
	}
	if f.Collection == "" {
		f.Collection = "-"
	}
	return f
}

func (s *SQLiteStore) ReplaceGeneSets(ctx context.Context, f GeneSetFilter, geneIDType string, sets types.GeneSets, version string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store not initialized")
	}
	f = normalizeFilter(f)
	if f.DB == "" || f.Species == "" {
		return fmt.Errorf("missing db/species for genesets replace")
	}
	if strings.TrimSpace(geneIDType) == "" {
		return fmt.Errorf("empty geneIDType")
	}
	if version == "" {
		version = "-"
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM geneset_gene WHERE db=? AND species=? AND ontology=? AND collection=?`,
		f.DB, f.Species, f.Ontology, f.Collection,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM geneset WHERE db=? AND species=? AND ontology=? AND collection=?`,
		f.DB, f.Species, f.Ontology, f.Collection,
	); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	insSet, err := tx.PrepareContext(ctx, `
		INSERT INTO geneset (db, species, ontology, collection, set_id, name, description, version, downloaded_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer insSet.Close()

	insGene, err := tx.PrepareContext(ctx, `
		INSERT INTO geneset_gene (db, species, ontology, collection, set_id, gene_id, gene_id_type)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer insGene.Close()

	for _, gs := range sets {
		if gs == nil || strings.TrimSpace(gs.ID) == "" {
			continue
		}
		name := gs.Name
		if strings.TrimSpace(name) == "" {
			name = gs.ID
		}
		desc := gs.Description
		if desc == "" {
			desc = "-"
		}
		if _, err := insSet.ExecContext(ctx,
			f.DB, f.Species, f.Ontology, f.Collection, gs.ID, name, desc, version, now,
		); err != nil {
			return err
		}
		for gene := range gs.Genes {
			gene = strings.TrimSpace(gene)
			if gene == "" {
				continue
			}
			if _, err := insGene.ExecContext(ctx,
				f.DB, f.Species, f.Ontology, f.Collection, gs.ID, gene, geneIDType,
			); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) LoadGeneSets(ctx context.Context, f GeneSetFilter) (types.GeneSets, string, error) {
	if s == nil || s.db == nil {
		return nil, "", fmt.Errorf("store not initialized")
	}
	f = normalizeFilter(f)
	if f.DB == "" || f.Species == "" {
		return nil, "", fmt.Errorf("missing db/species for genesets load")
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT g.set_id, g.name, g.description, gg.gene_id, gg.gene_id_type
		FROM geneset g
		JOIN geneset_gene gg
		  ON g.db=gg.db AND g.species=gg.species AND g.ontology=gg.ontology AND g.collection=gg.collection AND g.set_id=gg.set_id
		WHERE g.db=? AND g.species=? AND g.ontology=? AND g.collection=?
		ORDER BY g.set_id
	`, f.DB, f.Species, f.Ontology, f.Collection)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var sets types.GeneSets
	var lastID string
	var current *types.GeneSet
	var geneIDType string
	for rows.Next() {
		var setID, name, desc, gene, gidType string
		if err := rows.Scan(&setID, &name, &desc, &gene, &gidType); err != nil {
			return nil, "", err
		}
		if setID != lastID {
			if current != nil {
				sets = append(sets, current)
			}
			current = &types.GeneSet{
				ID:          setID,
				Name:        name,
				Description: desc,
				Genes:       make(map[string]bool),
			}
			lastID = setID
		}
		if current != nil && strings.TrimSpace(gene) != "" {
			current.Genes[gene] = true
		}
		if geneIDType == "" {
			geneIDType = gidType
		}
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if current != nil {
		sets = append(sets, current)
	}
	return sets, geneIDType, nil
}

