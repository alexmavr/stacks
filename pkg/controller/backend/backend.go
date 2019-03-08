package backend

import (
	"fmt"

	dockerTypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/swarm"

	"github.com/docker/stacks/pkg/compose/convert"
	"github.com/docker/stacks/pkg/compose/loader"
	composetypes "github.com/docker/stacks/pkg/compose/types"
	"github.com/docker/stacks/pkg/interfaces"
	"github.com/docker/stacks/pkg/substitution"
	"github.com/docker/stacks/pkg/types"
)

// DefaultStacksBackend implements the interfaces.StacksBackend interface, which serves as the
// API handler for the Stacks APIs.
type DefaultStacksBackend struct {
	// stackStore is the underlying CRUD store of stacks.
	stackStore interfaces.StackStore

	// swarmBackend provides access to swarmkit operations on secrets
	// and configs, required for stack validation and conversion.
	swarmBackend interfaces.SwarmResourceBackend
}

// NewDefaultStacksBackend creates a new DefaultStacksBackend.
func NewDefaultStacksBackend(stackStore interfaces.StackStore, swarmBackend interfaces.SwarmResourceBackend) *DefaultStacksBackend {
	return &DefaultStacksBackend{
		stackStore:   stackStore,
		swarmBackend: swarmBackend,
	}
}

// CreateStack creates a new stack if the stack is valid.
func (b *DefaultStacksBackend) CreateStack(create types.StackCreate) (types.StackCreateResponse, error) {
	if create.Orchestrator != types.OrchestratorSwarm {
		return types.StackCreateResponse{}, fmt.Errorf("invalid orchestrator type %s. This backend only supports orchestrator type swarm", create.Orchestrator)
	}

	// Create the Swarm Stack object
	stack := types.Stack{
		Spec:         create.Spec,
		Orchestrator: types.OrchestratorSwarm,
	}

	// Convert to the Stack to a SwarmStack
	swarmSpec, err := b.convertToSwarmStackSpec(create.Spec)
	if err != nil {
		return types.StackCreateResponse{}, fmt.Errorf("unable to translate swarm spec: %s", err)
	}

	swarmStack := interfaces.SwarmStack{
		Spec: swarmSpec,
	}

	id, err := b.stackStore.AddStack(stack, swarmStack)
	if err != nil {
		return types.StackCreateResponse{}, fmt.Errorf("unable to store stack: %s", err)
	}

	return types.StackCreateResponse{
		ID: id,
	}, err
}

// GetStack retrieves a stack by its ID.
func (b *DefaultStacksBackend) GetStack(id string) (types.Stack, error) {
	stack, err := b.stackStore.GetStack(id)
	if err != nil {
		return types.Stack{}, fmt.Errorf("unable to retrieve stack %s: %s", id, err)
	}

	return stack, err
}

// GetSwarmStack retrieves a swarm stack by its ID.
// NOTE: this is an internal-only method used by the Swarm Stacks Reconciler.
func (b *DefaultStacksBackend) GetSwarmStack(id string) (interfaces.SwarmStack, error) {
	stack, err := b.stackStore.GetSwarmStack(id)
	if err != nil {
		return interfaces.SwarmStack{}, fmt.Errorf("unable to retrieve swarm stack %s: %s", id, err)
	}

	return stack, err
}

// ListStacks lists all stacks.
func (b *DefaultStacksBackend) ListStacks() ([]types.Stack, error) {
	return b.stackStore.ListStacks()
}

// GetStackTasks retrieves a stacks tasks by its ID.
func (b *DefaultStacksBackend) GetStackTasks(id string) (types.StackTaskList, error) {
	return types.StackTaskList{}, nil
}

// getSwarmTasks returns all swarm tasks for a set of requested stackIDs.
func (b *DefaultStacksBackend) getSwarmTasks(stackIDs []string) (map[string][]swarm.Task, error) {
	// If a single stack's tasks has been requested, filter by that task's ID.
	// Otherwise, get all tasks with the StackLabel key.
	var f filters.Args
	switch len(stackIDs) {
	case 0:
		return map[string][]swarm.Task{}, nil
	case 1:
		f = interfaces.StackLabelFilter(stackIDs[0])
	default:
		f = filters.NewArgs(filters.Arg("label", interfaces.StackLabel))
	}

	// Generate the map using the requested stackIDs
	idsmap := make(map[string][]swarm.Task)
	for _, stackID := range stackIDs {
		idsmap[stackID] = []swarm.Task{}
	}

	tasks, err := b.swarmBackend.GetTasks(dockerTypes.TaskListOptions{
		Filters: f,
	})
	if err != nil {
		return idsmap, fmt.Errorf("unable to get tasks for stacks %+v: %s", stackIDs, err)
	}

	for _, task := range tasks {
		stackID, ok := task.Labels[interfaces.StackLabel]
		if !ok {
			return idsmap, fmt.Errorf("internal error: found task with no stack label despite label")
		}

		// Filter out tasks not from one of our desired stacks.
		stackTasks, found := idsmap[stackID]
		if found {
			idsmap[stackID] = append(stackTasks, task)
		}
	}

	return idsmap, nil
}

// ListSwarmStacks lists all swarm stacks.
// NOTE: this is an internal-only method used by the Swarm Stacks Reconciler.
func (b *DefaultStacksBackend) ListSwarmStacks() ([]interfaces.SwarmStack, error) {
	return b.stackStore.ListSwarmStacks()
}

// UpdateStack updates a stack.
func (b *DefaultStacksBackend) UpdateStack(id string, spec types.StackSpec, version uint64) error {
	// Convert the new StackSpec to a SwarmStackSpec, while retaining the
	// namespace label.
	swarmSpec, err := b.convertToSwarmStackSpec(spec)
	if err != nil {
		return fmt.Errorf("unable to translate swarm spec: %s", err)
	}

	return b.stackStore.UpdateStack(id, spec, swarmSpec, version)
}

// DeleteStack deletes a stack.
func (b *DefaultStacksBackend) DeleteStack(id string) error {
	return b.stackStore.DeleteStack(id)
}

// ParseComposeInput parses a compose file and returns the StackCreate object with the spec and any properties
func (b *DefaultStacksBackend) ParseComposeInput(input types.ComposeInput) (*types.StackCreate, error) {
	return loader.ParseComposeInput(input)
}

func (b *DefaultStacksBackend) convertToSwarmStackSpec(spec types.StackSpec) (interfaces.SwarmStackSpec, error) {
	// Substitute variables with desired property values
	substitutedSpec, err := substitution.DoSubstitution(spec)
	if err != nil {
		return interfaces.SwarmStackSpec{}, err
	}

	namespace := convert.NewNamespace(spec.Metadata.Name)

	services, err := convert.Services(namespace, substitutedSpec, b.swarmBackend)
	if err != nil {
		return interfaces.SwarmStackSpec{}, fmt.Errorf("failed to convert services : %s", err)
	}

	configs, err := convert.Configs(namespace, substitutedSpec.Configs)
	if err != nil {
		return interfaces.SwarmStackSpec{}, fmt.Errorf("failed to convert configs: %s", err)
	}

	secrets, err := convert.Secrets(namespace, substitutedSpec.Secrets)
	if err != nil {
		return interfaces.SwarmStackSpec{}, fmt.Errorf("failed to convert secrets: %s", err)
	}

	serviceNetworks := getServicesDeclaredNetworks(substitutedSpec.Services)
	networkCreates, _ := convert.Networks(namespace, substitutedSpec.Networks, serviceNetworks)
	// TODO: validate external networks?

	stackSpec := interfaces.SwarmStackSpec{
		Annotations: swarm.Annotations{
			Name:   spec.Metadata.Name,
			Labels: spec.Metadata.Labels,
		},
		Services: services,
		Configs:  configs,
		Secrets:  secrets,
		Networks: networkCreates,
	}

	return stackSpec, nil
}

func getServicesDeclaredNetworks(serviceConfigs []composetypes.ServiceConfig) map[string]struct{} {
	serviceNetworks := map[string]struct{}{}
	for _, serviceConfig := range serviceConfigs {
		if len(serviceConfig.Networks) == 0 {
			serviceNetworks["default"] = struct{}{}
			continue
		}
		for network := range serviceConfig.Networks {
			serviceNetworks[network] = struct{}{}
		}
	}
	return serviceNetworks
}

// TODO: rewrite if needed
/*
func validateExternalNetworks(
	ctx context.Context,
	client dockerclient.NetworkAPIClient,
	externalNetworks []string,
) error {
	for _, networkName := range externalNetworks {
		if !container.NetworkMode(networkName).IsUserDefined() {
			// Networks that are not user defined always exist on all nodes as
			// local-scoped networks, so there's no need to inspect them.
			continue
		}
		network, err := client.NetworkInspect(ctx, networkName, types.NetworkInspectOptions{})
		switch {
		case dockerclient.IsErrNotFound(err):
			return errors.Errorf("network %q is declared as external, but could not be found. You need to create a swarm-scoped network before the stack is deployed", networkName)
		case err != nil:
			return err
		case network.Scope != "swarm":
			return errors.Errorf("network %q is declared as external, but it is not in the right scope: %q instead of \"swarm\"", networkName, network.Scope)
		}
	}
	return nil
}
*/
