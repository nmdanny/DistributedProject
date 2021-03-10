$num_servers = 3
$threshold = 2
$num_clients = 3
$num_channels = 6

# all time units are in ms
$electionRange = '150..450'
$heartbeatInterval = 50

# length of share/recover phase (so round length is 2*phaseLength)
$phaseLength = 500

# grpc transport timeout for both client and server
$timeoutMs = 1000

$cur_dir = $PSScriptRoot

$start_cmds = @()

$exe = ".\target\release\runner.exe"

. .\set_logging.ps1
. .\start_servers.ps1