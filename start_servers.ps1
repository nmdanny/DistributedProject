$num_servers = 5
$threshold = 3
$num_clients = 3
$num_channels = 30

$timeoutMs = 10000

$cur_dir = $PSScriptRoot

$start_cmds = @()

$exe = ".\target\debug\runner.exe"

$arguments = @( "--title", "Main", "-d", """$cur_dir""", ";")

for($i=0; $i -lt $num_servers; $i++) {
    $cmd = "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels --timeout $timeoutMs server --id $i "
    $start_cmds += "new-tab --title ""Server $i"" -d ""$cur_dir"" $cmd"
}


for($i=0; $i -lt $num_clients; $i++) {
    $cmd = "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels --timeout $timeoutMs client -i $i "
    $start_cmds += "new-tab --title ""Client $i"" -d ""$cur_dir"" $cmd"
}



$arguments += $start_cmds -join ';'

Start-Process wt $arguments