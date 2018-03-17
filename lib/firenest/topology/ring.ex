defmodule Firenest.Topology.Ring do
  # TODO: Add docs
  @moduledoc false

  @behaviour Firenest.Topology
  @timeout 5_000

  defdelegate child_spec(opts), to: Firenest.Topology.Ring.Server

  def connect(topology, node) when is_atom(node) do
    fn ->
      subscribe(topology, self())
      case :net_kernel.connect(node) do
        true -> node in nodes(topology) or wait_until({:nodeup, node})
        false -> false
        :ignored -> :ignored
      end
    end
    |> Task.async()
    |> Task.await(:infinity)
  end

  def node(_topology) do
    Kernel.node()
  end

  def nodes(topology) do
    :ets.lookup_element(topology, :nodes, 2)
  end

  defp wait_until(msg) do
    receive do
      ^msg -> true
    after
      @timeout -> false
    end
  end

  defp subscribe(topology, pid) when is_pid(pid) do
    GenServer.call(topology, {:subscribe, pid})
  end
end

defmodule Firenest.Topology.Ring.Server do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    topology = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, topology, name: topology)
  end

  def init(topology) do
    setup_ets(topology)

    # We need to monitor nodes before we do the first broadcast.
    # Otherwise a node can come up between the first broadcast and
    # the first notification.
    :ok = :net_kernel.monitor_nodes(true, node_type: :hidden)

    # We generate a unique ID to be used alongside the node name
    # to guarantee uniqueness in case of restarts. Then we do a
    # broadcast over the Erlang topology to find other processes
    # like ours. The other server monitor states will be carried
    # in their pongs.
    id = id()
    # TODO: Should we enable this? We only want to connect to a single
    # node either way so might as well enforce that instead of pinging
    # Enum.each(Node.list(:hidden), &ping(&1, topology, id, []))

    state = %{
      topology: topology,
      nodes: %{},
      next: nil,
      id: id,
      monitors: %{},
      local_names: %{},
      subscribers: %{}
    }

    {:ok, state}
  end

  defp setup_ets(topology) do
    # Required ETS settings
    ^topology = :ets.new(topology, [:set, :public, :named_table, read_concurrency: true])
    true = :ets.insert(topology, {:adapter, Firenest.Topology.Ring})

    # Set up node names table
    true = :ets.insert(topology, {:nodes, %{}})
  end

  defp id do
    {:crypto.strong_rand_bytes(4), System.system_time()}
  end

  ## Local messages

  def handle_call({:subscribe, pid}, _from, state) do
    ref = Process.monitor(pid)
    state = put_in(state.subscribers[ref], pid)
    {:reply, ref, state}
  end

  ## Distributed messages

  def handle_info({:nodeup, node, _}, state) do
    %{topology: topology, id: id, monitors: monitors} = state
    ping(node, topology, id, Map.to_list(monitors))
    {:noreply, state}
  end

  def handle_info({:ping, other_id, pid, monitors}, %{id: id} = state) do
    pong(pid, id, Map.to_list(state.monitors))
    {:noreply, add_node(state, other_id, pid, monitors)}
  end

  def handle_info({:pong, other_id, pid, monitors}, state) do
    {:noreply, add_node(state, other_id, pid, monitors)}
  end

  def handle_info({:next, name, node}, state) do
    IO.inspect "my next is going to be: #{inspect {name, node}}"

    {:noreply, %{state | next: {name, node}}}
  end

  # TODO: Remove this, debugging purposes
  def handle_info(msg, state) do
    IO.inspect msg

    {:noreply, state}
  end

  ## Helpers

  defp ping(node, topology, id, monitors) when is_list(monitors) do
    send({topology, node}, {:ping, id, self(), monitors})
  end

  defp pong(pid, id, monitors) when is_list(monitors) do
    send(pid, {:pong, id, self(), monitors})
  end

  defp add_node(%{nodes: nodes} = state, id, pid, monitors) do
    node = Kernel.node(pid)
    new_remote_names = for {ref, name} <- monitors, do: {name, ref}, into: %{}

    case nodes do
      # TODO: If our next node crashed or disconnect we can connect to a random
      # node to keep the ring structure which falls into this branch
      %{^node => {^id, ref, old_remote_names}} ->
        :ok = diff_monitors(state, node, id, old_remote_names, monitors)
        state = put_in(state.nodes[node], {id, ref, new_remote_names})

      # TODO: In this branch the node is already part of the topology but is
      # trying to reconnect. It has a different ID so we might not have received
      # the down message yet. We should add it to the topology.
      # But should we remove it?
      %{^node => _} ->
        state
        |> delete_node_and_notify(node)
        |> add_node_and_notify(node, id, pid, new_remote_names, monitors)

      %{} ->
        add_node_and_notify(state, node, id, pid, new_remote_names, monitors)
    end
  end

  defp diff_monitors(state, node, id, remote_names, monitors) when is_list(monitors) do
    {added, removed} =
      Enum.reduce(monitors, {[], remote_names}, fn {ref, name}, {added, removed} ->
        case remote_names do
          %{^name => ^ref} ->
            {added, Map.delete(removed, ref)}
          %{} ->
            {[name | added], removed}
        end
      end)

    %{local_names: local_names} = state

    for {name, _ref} <- removed do
      local_monitor_down(state, node, id, name)
    end

    for name <- added do
      local_monitor_up(state, node, id, name)
    end

    :ok
  end

  defp add_node_and_notify(state, node, id, pid, remote_names, monitors) do
    %{
      topology: topology,
      nodes: nodes,
      local_names: local_names,
      subscribers: subscribers
    } = state

    # Add the node, notify the node, notify the services.
    nodes = Map.put(nodes, node, {id, Process.monitor(pid), remote_names})
    persist_node_names(topology, nodes)
    state = update_next(state, pid, node)

    _ = for {_, pid} <- subscribers, do: send(pid, {:nodeup, node})
    _ = for {_, name} <- monitors, do: local_monitor_up(state, node, id, name)
    %{state | nodes: nodes}
  end

  defp delete_node_and_notify(state, node) do
    %{
      topology: topology,
      nodes: nodes,
      local_names: local_names,
      subscribers: subscribers
    } = state

    # Notify the services, remove the node, notify the node.
    {id, _ref, remote_names} = Map.fetch!(nodes, node)
    _ = for {name, _} <- remote_names, do: local_monitor_down(state, node, id, name)

    nodes = Map.delete(nodes, node)
    persist_node_names(topology, nodes)

    _ = for {_, pid} <- subscribers, do: send(pid, {:nodedown, node})
    %{state | nodes: nodes}
  end

  # Our next is always going to be the new node, but to keep the ring structure
  # the new node must connect to our current next
  defp update_next(%{next: next} = state, pid, node) do
    # If we have no next node in the ring, send our information to the new node
    # If there is a next node, have the new node connect to it
    {next_name, next_node} = next || {state.topology, Kernel.node()}

    Process.send({state.topology, node}, {:next, next_name, next_node}, [:noconnect])
    # TODO: Node.disconnect without it thinking we crashed

    %{state | next: {pid, node}}
  end


  defp local_monitor_up(state, node, id, name) do
    %{local_names: local_names, id: own_id} = state
    from = {Kernel.node(), own_id}
    local_send(local_names, name, {:named_up, from, {node, id, name}})
  end

  defp local_monitor_down(state, node, id, name) do
    %{local_names: local_names, id: own_id} = state
    from = {Kernel.node(), own_id}
    local_send(local_names, name, {:named_down, from, {node, id, name}})
  end

  defp local_send(local_names, name, message) do
    case local_names do
      %{^name => {pid, _}} -> send(pid, message)
      %{} -> :error
    end

    :ok
  end

  defp persist_node_names(topology, nodes) do
    true = :ets.insert(topology, {:nodes, nodes |> Map.keys |> Enum.sort})
  end
end
