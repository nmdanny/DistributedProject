$num_servers = 8
$threshold = 5
$num_clients = 8
$num_channels = 30

# all time units are in ms
$electionRange = '1500..2000'
$heartbeatInterval = 100

# length of share/recover phase (so round length is 2*phaseLength)
$phaseLength = 500

# grpc transport timeout for both client and server
$timeoutMs = 1000

$cur_dir = $PSScriptRoot

$start_cmds = @()

$exe = ".\target\release\runner.exe"

$arguments = @( "--title", "Main", "-d", """$cur_dir""", ";")

for($i=0; $i -lt $num_servers; $i++) {
    $cmd = "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels --timeout $timeoutMs --phase_length $phaseLength server --id $i --heartbeat_interval $heartbeatInterval --election_range $electionRange"
    $start_cmds += "new-tab --title ""Server $i"" -d ""$cur_dir"" $cmd"
}


for($i=0; $i -lt $num_clients; $i++) {
    $cmd = "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels --timeout $timeoutMs --phase_length $phaseLength client -i $i "
    $start_cmds += "new-tab --title ""Client $i"" -d ""$cur_dir"" $cmd"
}



$arguments += $start_cmds -join ';'

Start-Process wt $arguments