ExUnit.start()
case RaftFleet.activate("zone") do
  :ok                     -> :timer.sleep(100)
  {:error, :not_inactive} -> :ok
end
