package types

// GeneSet 基因集
type GeneSet struct {
	ID          string
	Name        string
	Genes       map[string]bool // 基因 ID 集合
	Description string
}

// GeneSets 基因集集合
type GeneSets []*GeneSet
