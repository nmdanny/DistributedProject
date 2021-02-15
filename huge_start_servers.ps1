$num_servers = 2
$threshold = 2
$num_clients = 100
$num_channels = 1000

$cur_dir = $PSScriptRoot

$start_cmds = @()

$exe = ".\target\debug\runner.exe"
# $exe = ".\target\release\runner.exe"

$arguments = @( "--title", "Main", "-d", """$cur_dir""", ";")

for($i=0; $i -lt $num_servers; $i++) {
    $cmd = "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels server --id $i "
    $start_cmds += "new-tab --title ""Server $i"" -d ""$cur_dir"" $cmd"
}


$arguments += $start_cmds -join ';'

Start-Process wt $arguments