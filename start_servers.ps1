$num_servers = 5

$cur_dir = $PSScriptRoot

$start_cmds = @()

$arguments = @( "--title", "Main", "-d", "`"$cur_dir`"", ";")

for($i=0; $i -lt $num_servers; $i++) {
    $cmd = "cargo run --bin runner -- server --id $i "
    $start_cmds += "new-tab --title ""Server $i"" -d ""$cur_dir"" $cmd"
}


$arguments += $start_cmds -join ';'

Start-Process wt $arguments