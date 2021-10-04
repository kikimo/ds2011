package shardctrler

import (
	"fmt"
	"sort"
)

// type Config struct {
// 	Num    int              // config number
// 	Shards [NShards]int     // shard -> gid
// 	Groups map[int][]string // gid -> servers[]
// }

type GroupShard struct {
	gid    int
	shards []int
}

// TODO unit test me
func (c *Config) buildGroupShard() ([]GroupShard, []int) {
	// map gid to shards
	gShardMap := map[int][]int{}
	freeShards := []int{}
	for shard, gid := range c.Shards {
		if gid != 0 {
			gShardMap[gid] = append(gShardMap[gid], shard)
		} else {
			freeShards = append(freeShards, shard)
		}
	}
	// fmt.Printf("gshard map: %+v\n", gShardMap)

	// include groups that are no allocated to any shards
	for gid := range c.Groups {
		if _, ok := gShardMap[gid]; !ok {
			gShardMap[gid] = []int{}
		}
	}

	// build GroupShard array
	gShards := []GroupShard{}
	for gid, shards := range gShardMap {
		gShard := GroupShard{
			gid:    gid,
			shards: shards,
		}
		gShards = append(gShards, gShard)
	}

	return gShards, freeShards
}

func (c *Config) Clone() *Config {
	shards := [NShards]int{}
	for i := 0; i < NShards; i++ {
		shards[i] = c.Shards[i]
	}

	copyServer := func(servers []string) []string {
		sz := len(servers)
		copiedServers := make([]string, sz)
		copy(copiedServers, servers)

		return copiedServers
	}
	groups := map[int][]string{}
	for gid, servers := range c.Groups {
		copiedServers := copyServer(servers)
		groups[gid] = copiedServers
	}

	cfg := &Config{
		Num:    c.Num,
		Shards: shards,
		Groups: groups,
	}

	return cfg
}

// BalanceShard divids shards among groups as evenly as possible
// and move as few shards as possible.
func (c *Config) BalanceShard() {
	gShards, freeShards := c.buildGroupShard()

	// sort GroupShard by size of shards in descending order
	sort.Slice(gShards, func(i, j int) bool {
		szi, szj := len(gShards[i].shards), len(gShards[j].shards)
		if szi > szj {
			return true
		}

		if szi == szj {
			return gShards[i].gid < gShards[j].gid
		}

		return false
	})

	// now balance shards among groups
	groupNum := len(c.Groups)
	shardPerGroup := NShards / groupNum
	shardAlloc := make([]int, groupNum) // [gid]numberShards
	for i := range shardAlloc {
		shardAlloc[i] = shardPerGroup
	}

	extraShards := NShards % groupNum
	for i := 0; i < extraShards; i++ {
		shardAlloc[i]++
	}
	// fmt.Printf("shard alloc: %+v, sz: %d\n", shardAlloc, len(shardAlloc))
	// fmt.Printf("gShard: %+v\n", gShards)

	for i := 0; i < groupNum; i++ {
		sz := len(gShards[i].shards)
		ideaSz := shardAlloc[i]
		if sz == ideaSz {
			break
		} else if sz > ideaSz {
			// reserve extra shards from group
			freeShards = append(freeShards, gShards[i].shards[ideaSz:]...)
			gShards[i].shards = gShards[i].shards[:ideaSz]
		} else { // sz < ideaSz
			// alloc shards from freeShards to group
			need := ideaSz - sz
			if len(freeShards) < need {
				panic(fmt.Sprintf("unreachable, need %d free shards but only got %d", need, len(freeShards)))
			}

			gShards[i].shards = append(gShards[i].shards, freeShards[:need]...)
			freeShards = freeShards[need:]
		}
	}

	// check resut
	if len(freeShards) != 0 {
		panic(fmt.Sprintf("%d shards not allocated", len(freeShards)))
	}

	for i := 0; i < groupNum; i++ {
		if len(gShards[i].shards) != shardAlloc[i] {
			panic(fmt.Sprintf("plan to allocate %d shared to group %d but foundi %d", shardAlloc[i], gShards[i].gid, len(gShards[i].shards)))
		}
	}

	// now map shards to group again
	for _, gShard := range gShards {
		for _, shard := range gShard.shards {
			c.Shards[shard] = gShard.gid
		}
	}
}

// func (c *Config) String() string {

// }
