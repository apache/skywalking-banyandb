package physical

type tableScan struct {
}

type chunkIDFetch struct {
}

type traceFetch struct {
}

type indexScan struct {
}

type sortedMerge struct {
}

type pagination struct {
	offset uint32
	limit  uint32
}
