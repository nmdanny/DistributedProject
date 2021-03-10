$num_servers = 8
$threshold = 5
$num_clients = 3
$num_channels = 3

# all time units are in ms
$electionRange = '1500..2000'
$heartbeatInterval = 100

# length of share/recover phase (so round length is 2*phaseLength)
$phaseLength = 1000

# grpc transport timeout for both client and server
$timeoutMs = 2000

$cur_dir = $PSScriptRoot

$start_cmds = @()

$exe = ".\target\release\runner.exe"

. .\set_logging.ps1
. .\start_servers.ps1