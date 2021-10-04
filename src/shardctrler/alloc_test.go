package shardctrler

import "testing"

func TestShardAllocation(t *testing.T) {
	cases := []struct {
		name           string
		cfg            Config
		expectedShards [NShards]int
	}{
		{
			name: "",
			cfg: Config{
				Shards: [NShards]int{},
				Groups: map[int][]string{
					1: nil,
				},
			},
			expectedShards: [NShards]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			name: "",
			cfg: Config{
				Shards: [NShards]int{},
				Groups: map[int][]string{
					1: nil,
					2: nil,
					3: nil,
				},
			},
			expectedShards: [NShards]int{1, 1, 1, 1, 2, 2, 2, 3, 3, 3},
		},
		{
			name: "",
			cfg: Config{
				Shards: [NShards]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
				Groups: map[int][]string{
					1: nil,
					2: nil,
					3: nil,
				},
			},
			expectedShards: [NShards]int{1, 1, 1, 1, 2, 2, 2, 3, 3, 3},
		},
		{
			name: "",
			cfg: Config{
				Shards: [NShards]int{1, 1, 1, 1, 1, 1, 1, 1, 2, 3},
				Groups: map[int][]string{
					1: nil,
					2: nil,
					3: nil,
				},
			},
			expectedShards: [NShards]int{1, 1, 1, 1, 2, 2, 3, 3, 2, 3},
		},
		{
			name: "",
			cfg: Config{
				Shards: [NShards]int{1, 1, 1, 1, 1, 1, 1, 1, 2, 2},
				Groups: map[int][]string{
					1: nil,
					2: nil,
					3: nil,
				},
			},
			expectedShards: [NShards]int{1, 1, 1, 1, 2, 3, 3, 3, 2, 2},
		},
	}

	for _, c := range cases {
		cfg := c.cfg.Clone()
		cfg.BalanceShard()
		if cfg.Shards != c.expectedShards {
			t.Fatalf("Failed testing shard allocation: %s, expect %+v but got %+v", c.name, c.expectedShards, cfg.Shards)
		}
	}
}
