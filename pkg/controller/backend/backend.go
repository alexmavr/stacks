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

func (b *DefaultStacksBackend) getNodeCount() (uint64, error) {
	nodes, err := b.swarmBackend.GetNodes(dockerTypes.NodeListOptions{})
	if err != nil {
		return 0, fmt.Errorf("unable to list nodes: %s", err)
	}

	return uint64(len(nodes)), nil
}

func (b *DefaultStacksBackend) getStackStatuses(stacks []types.Stack) ([]types.Stack, error) {
	if len(stacks) == 0 {
		return []types.Stack{}, nil
	}

	stackIDs := make([]string, len(stacks))
	for i, stack := range stacks {
		stackIDs[i] = stack.ID
	}

	// Get all the services, secrets, configs, networks and tasks
	tasks, err := b.getSwarmTasks(stackIDs)
	if err != nil {
		return []types.Stack{}, fmt.Errorf("unable to get swarm tasks: %s", err)
	}

	services, err := b.getSwarmServices(stackIDs)
	if err != nil {
		return []types.Stack{}, fmt.Errorf("unable to get swarm services: %s", err)
	}

	secrets, err := b.getSwarmSecrets(stackIDs)
	if err != nil {
		return []types.Stack{}, fmt.Errorf("unable to get swarm secrets: %s", err)
	}

	configs, err := b.getSwarmConfigs(stackIDs)
	if err != nil {
		return []types.Stack{}, fmt.Errorf("unable to get swarm secrets: %s", err)
	}

	// TODO: also get networks
	// TODO: also get volumes

	nodeCount, err := b.getNodeCount()
	if err != nil {
		return []types.Stack{}, fmt.Errorf("unable to get nodes: %s", err)
	}

	newStacks := make([]types.Stack, len(stacks))
	for _, stack := range stacks {
		stackTasks, ok := tasks[stack.ID]
		if !ok {
			panic("internal error: task map does not contain expected ID")
		}

		stackServices, ok := services[stack.ID]
		if !ok {
			panic("internal error: services map does not contain expected ID")
		}

		stackSecrets, ok := secrets[stack.ID]
		if !ok {
			panic("internal error: services map does not contain expected ID")
		}

		stackConfigs, ok := configs[stack.ID]
		if !ok {
			panic("internal error: configs map does not contain expected ID")
		}

		// TODO: all resources
		err := populateStackStatus(&stack, stackTasks, stackServices, stackSecrets, stackConfigs, nodeCount)
		if err != nil {
			return []types.Stack{}, fmt.Errorf("unable to populate stack status for stack %s: %s", stack.ID, err)
		}

		newStacks = append(newStacks, stack)
	}

	return newStacks, nil

}

func populateStackStatus(stack *types.Stack, tasks []swarm.Task, services []swarm.Service, secrets []swarm.Secret, configs []swarm.Config, numNodes uint64) error {
	stack.Resources = types.StackResources{
		Services: stackServices(services),
		Secrets:  stackSecrets(secrets),
		Configs:  stackConfigs(configs),
	}

	for _, service := range services {
		// Determine the desired tasks for the service
		var desiredTasks uint64
		if service.Spec.Mode.Replicated != nil {
			desiredTasks = *service.Spec.Mode.Replicated.Replicas
		} else {
			// A Global mode service has a desired number of tasks equal to the
			// number of nodes.
			desiredTasks = numNodes
		}

		// Determine the "current" tasks for the service
		currentTasks := 0
		for _, task := range tasks {
			if task.ServiceID == service.ID && task.Status.State == swarm.TaskStateRunning {
				curentTasks++
			}
		}

		stack.Status.ServicesStatus[service.Spec.Name] = types.ServiceStatus{
			CurrentTasks: currentTasks,
			DesiredTasks: desiredTasks,
		}
	}

	// Calculate the overall status from the ServicesStatus field
	// TODO

	return nil
}

func stackServices(services []swarm.Service) map[string]types.StackResource {
	res := make(map[string]types.StackResource)
	for _, service := range services {
		res[service.Spec.Name] = types.StackResource{
			ID:   service.ID,
			Kind: types.KindSwarmService,
		}
	}
	return res
}

func stackSecrets(secrets []swarm.Secret) map[string]types.StackResource {
	res := make(map[string]types.StackResource)
	for _, secret := range secrets {
		res[secret.Spec.Name] = types.StackResource{
			ID:   secret.ID,
			Kind: types.KindSwarmSecret,
		}
	}
	return res
}

func stackConfigs(configs []swarm.Config) map[string]types.StackResource {
	res := make(map[string]types.StackResource)
	for _, config := range configs {
		res[config.Spec.Name] = types.StackResource{
			ID:   config.ID,
			Kind: types.KindSwarmConfig,
		}
	}

	return res
}

// getSwarmServices returns all swarm secrets for a set of requested stackIDs.
func (b *DefaultStacksBackend) getSwarmServices(stackIDs []string) (map[string][]swarm.Service, error) {
	services, err := b.swarmBackend.GetServices(dockerTypes.ServiceListOptions{
		Filters: stackLabelFilters(stackIDs),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list services: %s", err)
	}

	// Generate the map using the requested stackIDs
	idsmap := make(map[string][]swarm.Service)
	for _, stackID := range stackIDs {
		idsmap[stackID] = []swarm.Service{}
	}

	for _, service := range services {
		stackID, ok := service.Spec.Annotations.Labels[interfaces.StackLabel]
		if !ok {
			return idsmap, fmt.Errorf("internal error: found service with no stack label")
		}

		// Filter out services not from one of our desired stacks.
		stackServices, found := idsmap[stackID]
		if found {
			idsmap[stackID] = append(stackServices, service)
		}
	}

	return idsmap, nil
}

// getSwarmConfigs returns all swarm configs for a set of requested stackIDs.
func (b *DefaultStacksBackend) getSwarmConfigs(stackIDs []string) (map[string][]swarm.Config, error) {
	configs, err := b.swarmBackend.GetConfigs(dockerTypes.ConfigListOptions{
		Filters: stackLabelFilters(stackIDs),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list configs: %s", err)
	}

	// Generate the map using the requested stackIDs
	idsmap := make(map[string][]swarm.Config)
	for _, stackID := range stackIDs {
		idsmap[stackID] = []swarm.Config{}
	}

	for _, config := range configs {
		stackID, ok := config.Spec.Annotations.Labels[interfaces.StackLabel]
		if !ok {
			return idsmap, fmt.Errorf("internal error: found service with no stack label")
		}

		// Filter out services not from one of our desired stacks.
		stackConfigs, found := idsmap[stackID]
		if found {
			idsmap[stackID] = append(stackConfigs, config)
		}
	}

	return idsmap, nil
}

// getSwarmSecrets returns all swarm secrets for a set of requested stackIDs.
func (b *DefaultStacksBackend) getSwarmSecrets(stackIDs []string) (map[string][]swarm.Secret, error) {
	secrets, err := b.swarmBackend.GetSecrets(dockerTypes.SecretListOptions{
		Filters: stackLabelFilters(stackIDs),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list secrets: %s", err)
	}

	// Generate the map using the requested stackIDs
	idsmap := make(map[string][]swarm.Secret)
	for _, stackID := range stackIDs {
		idsmap[stackID] = []swarm.Secret{}
	}

	for _, secret := range secrets {
		stackID, ok := secret.Spec.Annotations.Labels[interfaces.StackLabel]
		if !ok {
			return idsmap, fmt.Errorf("internal error: found secret with no stack label")
		}

		// Filter out secrets not from one of our desired stacks.
		stackSecrets, found := idsmap[stackID]
		if found {
			idsmap[stackID] = append(stackSecrets, secret)
		}
	}

	return idsmap, nil
}

func stackLabelFilters(stackIDs []string) filters.Args {
	// If a single stack's tasks has been requested, filter by that task's ID.
	// Otherwise, get all tasks with the StackLabel key.
	if len(stackIDs) == 1 {
		return interfaces.StackLabelFilter(stackIDs[0])
	}
	return filters.NewArgs(filters.Arg("label", interfaces.StackLabel))
}

// getSwarmTasks returns all swarm tasks for a set of requested stackIDs.
func (b *DefaultStacksBackend) getSwarmTasks(stackIDs []string) (map[string][]swarm.Task, error) {
	tasks, err := b.swarmBackend.GetTasks(dockerTypes.TaskListOptions{
		Filters: stackLabelFilters(stackIDs),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get tasks for stacks %+v: %s", stackIDs, err)
	}

	// Generate the map using the requested stackIDs
	idsmap := make(map[string][]swarm.Task)
	for _, stackID := range stackIDs {
		idsmap[stackID] = []swarm.Task{}
	}

	for _, task := range tasks {
		stackID, ok := task.Labels[interfaces.StackLabel]
		if !ok {
			return idsmap, fmt.Errorf("internal error: found task with no stack label")
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
