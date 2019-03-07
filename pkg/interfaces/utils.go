package interfaces

import (
	"fmt"

	"github.com/docker/docker/api/types/filters"
)

// StackLabelFilter constructs a filter.Args which filters for stacks based on
// the stack label being equal to the stack ID.
func StackLabelFilter(stackID string) filters.Args {
	if stackID == "" {
		return filters.NewArgs()
	}

	return filters.NewArgs(
		filters.Arg("label", fmt.Sprintf("%s=%s", StackLabel, stackID)),
	)
}
