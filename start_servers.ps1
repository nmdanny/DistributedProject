$num_servers = 5
$threshold = 3
$num_clients = 3
$num_channels = 30

$phaseLength = 500
$timeoutMs = 1000

$cur_dir = $PSScriptRoot

$start_cmds = @()

$exe = ".\target\release\runner.exe"

$arguments = @( "--title", "Main", "-d", """$cur_dir""", ";")

for($i=0; $i -lt $num_servers; $i++) {
    $cmd = "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels --timeout $timeoutMs --phase_length $phaseLength server --id $i "
    $start_cmds += "new-tab --title ""Server $i"" -d ""$cur_dir"" $cmd"
}


for($i=0; $i -lt $num_clients; $i++) {
    $cmd = "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels --timeout $timeoutMs --phase_length $phaseLength client -i $i "
    $start_cmds += "new-tab --title ""Client $i"" -d ""$cur_dir"" $cmd"
}



$arguments += $start_cmds -join ';'

Start-Process wt $arguments