package interfaces

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStackLabelFilterEmpty(t *testing.T) {
	require := require.New(t)

	// Empty Filter
	f := StackLabelFilter("")
	require.Equal(f.Len(), 0)

	f = StackLabelFilter("stackid")
	require.Equal(f.Len(), 1)
	r := f.Get("label")
	require.Len(r, 1)
	require.Equal(r[0], fmt.Sprintf("%s=stackid", StackLabel))
}
