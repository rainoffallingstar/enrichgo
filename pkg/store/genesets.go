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
	geneIDType = strings.TrimSpace(geneIDType)
	if geneIDType == "" {
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

	now := time.Now().UTC().Format(time.RFC3339)
	datasetID, err := ensureDatasetForReplace(ctx, tx, f, geneIDType, version, now)
	if err != nil {
		return err
	}

	insTerm, err := tx.PrepareContext(ctx, `
		INSERT INTO term (dataset_id, term_id, name, description)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer insTerm.Close()

	insTermGene, err := tx.PrepareContext(ctx, `
		INSERT INTO term_gene (dataset_id, term_id, gene_pk)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer insTermGene.Close()

	insGeneDict, err := tx.PrepareContext(ctx, `
		INSERT OR IGNORE INTO gene_dict (gene_id)
		VALUES (?)
	`)
	if err != nil {
		return err
	}
	defer insGeneDict.Close()

	selGenePK, err := tx.PrepareContext(ctx, `
		SELECT gene_pk
		FROM gene_dict
		WHERE gene_id=?
	`)
	if err != nil {
		return err
	}
	defer selGenePK.Close()

	genePKCache := make(map[string]int64)
	resolveGenePK := func(gene string) (int64, error) {
		if pk, ok := genePKCache[gene]; ok {
			return pk, nil
		}
		if _, err := insGeneDict.ExecContext(ctx, gene); err != nil {
			return 0, err
		}
		var pk int64
		if err := selGenePK.QueryRowContext(ctx, gene).Scan(&pk); err != nil {
			return 0, err
		}
		genePKCache[gene] = pk
		return pk, nil
	}

	for _, gs := range sets {
		if gs == nil || strings.TrimSpace(gs.ID) == "" {
			continue
		}
		name := strings.TrimSpace(gs.Name)
		if name == "" {
			name = gs.ID
		}
		desc := gs.Description
		if desc == "" {
			desc = "-"
		}
		if _, err := insTerm.ExecContext(ctx, datasetID, gs.ID, name, desc); err != nil {
			return err
		}
		for gene := range gs.Genes {
			gene = strings.TrimSpace(gene)
			if gene == "" {
				continue
			}
			pk, err := resolveGenePK(gene)
			if err != nil {
				return err
			}
			if _, err := insTermGene.ExecContext(ctx, datasetID, gs.ID, pk); err != nil {
				return err
			}
		}
	}

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM gene_dict
		WHERE gene_pk NOT IN (SELECT DISTINCT gene_pk FROM term_gene)
	`); err != nil {
		return err
	}

	return tx.Commit()
}

func ensureDatasetForReplace(ctx context.Context, tx *sql.Tx, f GeneSetFilter, geneIDType, version, now string) (int64, error) {
	var datasetID int64
	err := tx.QueryRowContext(ctx, `
		SELECT id
		FROM dataset
		WHERE db=? AND species=? AND ontology=? AND collection=?
	`, f.DB, f.Species, f.Ontology, f.Collection).Scan(&datasetID)
	if err == nil {
		if _, err := tx.ExecContext(ctx, `DELETE FROM term_gene WHERE dataset_id=?`, datasetID); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM term WHERE dataset_id=?`, datasetID); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE dataset
			SET gene_id_type=?, version=?, downloaded_at=?
			WHERE id=?
		`, geneIDType, version, now, datasetID); err != nil {
			return 0, err
		}
		return datasetID, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO dataset (db, species, ontology, collection, gene_id_type, version, downloaded_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, f.DB, f.Species, f.Ontology, f.Collection, geneIDType, version, now)
	if err != nil {
		return 0, err
	}
	datasetID, err = res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return datasetID, nil
}

func (s *SQLiteStore) LoadGeneSets(ctx context.Context, f GeneSetFilter) (types.GeneSets, string, error) {
	if s == nil || s.db == nil {
		return nil, "", fmt.Errorf("store not initialized")
	}
	f = normalizeFilter(f)
	if f.DB == "" || f.Species == "" {
		return nil, "", fmt.Errorf("missing db/species for genesets load")
	}

	var datasetID int64
	var geneIDType string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, gene_id_type
		FROM dataset
		WHERE db=? AND species=? AND ontology=? AND collection=?
	`, f.DB, f.Species, f.Ontology, f.Collection).Scan(&datasetID, &geneIDType)
	if err == sql.ErrNoRows {
		return types.GeneSets{}, "", nil
	}
	if err != nil {
		return nil, "", err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT t.term_id, t.name, t.description, gd.gene_id
		FROM term t
		LEFT JOIN term_gene tg
		  ON t.dataset_id=tg.dataset_id AND t.term_id=tg.term_id
		LEFT JOIN gene_dict gd
		  ON tg.gene_pk=gd.gene_pk
		WHERE t.dataset_id=?
		ORDER BY t.term_id
	`, datasetID)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var sets types.GeneSets
	var lastID string
	var current *types.GeneSet
	for rows.Next() {
		var setID, name, desc string
		var gene sql.NullString
		if err := rows.Scan(&setID, &name, &desc, &gene); err != nil {
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
		if current != nil && gene.Valid {
			g := strings.TrimSpace(gene.String)
			if g != "" {
				current.Genes[g] = true
			}
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
