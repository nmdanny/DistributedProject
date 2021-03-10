$cur_dir = $PSScriptRoot
$start_cmds = @()
$arguments = @( "--title", "Main", "-d", """$cur_dir""", ";")

if ($null -eq $exe)
{
    Write-Error "You shouldn't use this script directly, use one of the `start_servers_{debug|release}_xxx.ps1` scripts"
    exit
}

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