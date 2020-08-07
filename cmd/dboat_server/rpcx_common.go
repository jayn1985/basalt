package main

// BitmapValueRequest contains the name of bitmap and value.
type BitmapValueRequest struct {
	Name  string
	Value uint32
}

// BitmapValuesRequest contains the name of bitmap and values.
type BitmapValuesRequest struct {
	Name   string
	Values []uint32
}

// BitmapStoreRequest contains the name of destination and names of bitmaps.
type BitmapStoreRequest struct {
	Destination string
	Names       []string
}

// BitmapPairRequest contains the name of two bitmaps.
type BitmapPairRequest struct {
	Name1 string
	Name2 string
}

// BitmapDstAndPairRequest contains  destination and the name of two bitmaps.
type BitmapDstAndPairRequest struct {
	Destination string
	Name1       string
	Name2       string
}
