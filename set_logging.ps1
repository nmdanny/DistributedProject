$FILTERS = @(
    "dist_lib::consensus::client=error",
    "dist_lib::consensus=debug",
    "dist_lib::consensus::leader=info"
    "dist_lib::anonymity=warn"
)


$env:RUST_LOG = [String]::Join(",", $FILTERS)