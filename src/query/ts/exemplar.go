package ts

// Exemplar holds the exemplar information for a datapoint.
type Exemplar []byte

// Exemplars is an array of exemplars.
type Exemplars []Exemplar

// ExemplarList represents a slice of exemplar list pointers.
type ExemplarsList []*Exemplars
