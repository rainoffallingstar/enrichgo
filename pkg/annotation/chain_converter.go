package annotation

import (
	"fmt"
	"strings"
	"sync"
)

type chainLayer struct {
	name      string
	converter IDConverter
}

// ChainIDConverter runs multiple converters in order and keeps the first
// successful conversion result for each input ID.
type ChainIDConverter struct {
	layers    []chainLayer
	layerHits map[string]int
	mu        sync.RWMutex
}

func NewChainIDConverter() *ChainIDConverter {
	return &ChainIDConverter{}
}

func (c *ChainIDConverter) AddLayer(name string, converter IDConverter) {
	if c == nil || converter == nil {
		return
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = fmt.Sprintf("layer_%d", len(c.layers)+1)
	}
	c.layers = append(c.layers, chainLayer{name: name, converter: converter})
}

func (c *ChainIDConverter) LayerStats() map[string]int {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]int, len(c.layerHits))
	for k, v := range c.layerHits {
		out[k] = v
	}
	return out
}

func (c *ChainIDConverter) Convert(geneIDs []string, fromType, toType IDType, species string) (map[string][]string, error) {
	if c == nil || len(c.layers) == 0 {
		return nil, fmt.Errorf("chain converter has no layers")
	}

	remaining := uniqueInputOrder(geneIDs)
	resolved := make(map[string][]string, len(remaining))
	stats := make(map[string]int)

	for _, layer := range c.layers {
		if len(remaining) == 0 {
			break
		}
		mapping, err := layer.converter.Convert(remaining, fromType, toType, species)
		if err != nil {
			continue
		}

		next := make([]string, 0, len(remaining))
		for _, orig := range remaining {
			ids := mapping[orig]
			if hasConvertedID(orig, ids) {
				resolved[orig] = uniqueStrings(ids)
				stats[layer.name]++
				continue
			}
			next = append(next, orig)
		}
		remaining = next
	}

	for _, orig := range remaining {
		resolved[orig] = []string{orig}
	}
	for _, orig := range geneIDs {
		if _, ok := resolved[orig]; !ok {
			resolved[orig] = []string{orig}
		}
	}

	c.mu.Lock()
	c.layerHits = stats
	c.mu.Unlock()
	return resolved, nil
}

func uniqueInputOrder(ids []string) []string {
	seen := make(map[string]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}

func hasConvertedID(orig string, ids []string) bool {
	for _, id := range ids {
		if strings.TrimSpace(id) != "" && id != orig {
			return true
		}
	}
	return false
}
